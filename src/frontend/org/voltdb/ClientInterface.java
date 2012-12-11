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

package org.voltdb;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.ZooDefs.Ids;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.BinaryPayloadMessage;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.messaging.LocalObjectMessage;
import org.voltcore.messaging.Mailbox;
import org.voltcore.messaging.VoltMessage;
import org.voltcore.network.Connection;
import org.voltcore.network.InputHandler;
import org.voltcore.network.NIOReadStream;
import org.voltcore.network.QueueMonitor;
import org.voltcore.network.VoltNetworkPool;
import org.voltcore.network.VoltProtocolHandler;
import org.voltcore.network.WriteStream;
import org.voltcore.utils.COWMap;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.DeferredSerialization;
import org.voltcore.utils.EstTime;
import org.voltcore.utils.Pair;
import org.voltdb.ClientInterfaceHandleManager.Iv2InFlight;
import org.voltdb.SystemProcedureCatalog.Config;
import org.voltdb.VoltZK.MailboxType;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.SnapshotSchedule;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.Table;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureInvocationType;
import org.voltdb.compiler.AdHocPlannedStatement;
import org.voltdb.compiler.AdHocPlannedStmtBatch;
import org.voltdb.compiler.AdHocPlannerWork;
import org.voltdb.compiler.AsyncCompilerResult;
import org.voltdb.compiler.AsyncCompilerWork.AsyncCompilerWorkCompletionHandler;
import org.voltdb.compiler.CatalogChangeResult;
import org.voltdb.compiler.CatalogChangeWork;
import org.voltdb.dtxn.InitiatorStats.InvocationInfo;
import org.voltdb.dtxn.SimpleDtxnInitiator;
import org.voltdb.dtxn.TransactionInitiator;
import org.voltdb.export.ExportManager;
import org.voltdb.iv2.BaseInitiator;
import org.voltdb.iv2.Cartographer;
import org.voltdb.iv2.Iv2Trace;
import org.voltdb.iv2.LeaderCache;
import org.voltdb.iv2.LeaderCacheReader;
import org.voltdb.iv2.MpInitiator;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.messaging.FastSerializer;
import org.voltdb.messaging.InitiateResponseMessage;
import org.voltdb.messaging.Iv2EndOfLogMessage;
import org.voltdb.messaging.Iv2InitiateTaskMessage;
import org.voltdb.messaging.LocalMailbox;
import org.voltdb.messaging.MultiPartitionParticipantMessage;
import org.voltdb.plannodes.PlanNodeTree;
import org.voltdb.plannodes.SendPlanNode;
import org.voltdb.sysprocs.LoadSinglepartitionTable;
import org.voltdb.utils.Encoder;
import org.voltdb.utils.LogKeys;
import org.voltdb.utils.MiscUtils;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.google.common.util.concurrent.MoreExecutors;

/**
 * Represents VoltDB's connection to client libraries outside the cluster.
 * This class accepts new connections and manages existing connections through
 * <code>ClientConnection</code> instances.
 *
 */
public class ClientInterface implements SnapshotDaemon.DaemonInitiator {

    // reasons a connection can fail
    public static final byte AUTHENTICATION_FAILURE = -1;
    public static final byte MAX_CONNECTIONS_LIMIT_ERROR = 1;
    public static final byte WIRE_PROTOCOL_TIMEOUT_ERROR = 2;
    public static final byte WIRE_PROTOCOL_FORMAT_ERROR = 3;
    public static final byte AUTHENTICATION_FAILURE_DUE_TO_REJOIN = 4;
    public static final byte EXPORT_DISABLED_REJECTION = 5;

    private static final VoltLogger log = new VoltLogger(ClientInterface.class.getName());
    private static final VoltLogger authLog = new VoltLogger("AUTH");
    private static final VoltLogger hostLog = new VoltLogger("HOST");
    private static final VoltLogger networkLog = new VoltLogger("NETWORK");
    private final ClientAcceptor m_acceptor;
    private ClientAcceptor m_adminAcceptor;
    private final TransactionInitiator m_initiator;

    /*
     * This lock must be held while checking and signaling a backpressure condition
     * in order to avoid ensure that nothing misses the end of backpressure notification
     */
    private final ReentrantLock m_backpressureLock = new ReentrantLock();
    private final CopyOnWriteArrayList<Connection> m_connections = new CopyOnWriteArrayList<Connection>();
    private final SnapshotDaemon m_snapshotDaemon = new SnapshotDaemon();
    private final SnapshotDaemonAdapter m_snapshotDaemonAdapter = new SnapshotDaemonAdapter();

    // Atomically allows the catalog reference to change between access
    private final AtomicReference<CatalogContext> m_catalogContext = new AtomicReference<CatalogContext>(null);

    /** If this is true, update the catalog */
    private final AtomicBoolean m_shouldUpdateCatalog = new AtomicBoolean(false);

    /**
     * Counter of the number of client connections. Used to enforce a limit on the maximum number of connections
     */
    private final AtomicInteger m_numConnections = new AtomicInteger(0);

    /**
     * IV2 stuff
     */
    private final LeaderCacheReader m_iv2Masters;

    /**
     * ZooKeeper is used for @Promote to trigger a truncation snapshot.
     */
    ZooKeeper m_zk;

    /**
     * The CIHM is unique to the connection and the ACG is shared by all connections
     * serviced by the associated network thread. They are paired so as to only do a single
     * lookup.
     */
    private final COWMap<Long, ClientInterfaceHandleManager>
            m_cihm = new COWMap<Long, ClientInterfaceHandleManager>();
    private final Cartographer m_cartographer;

    /**
     * Policies used to determine if we can accept an invocation.
     */
    private final Map<String, List<InvocationAcceptancePolicy>> m_policies =
            new HashMap<String, List<InvocationAcceptancePolicy>>();

    /**
     * For IV2 only: this is used to track the last txnId replicated for a
     * certain partition, so that duped DR txns can be dropped.
     */
    private final Map<Integer, Long> m_partitionTxnIds = new HashMap<Integer, Long>();

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

    // clock time of last call to the initiator's tick()
    static final int POKE_INTERVAL = 1000;

    private final int m_allPartitions[];
    private ImmutableMap<Integer, Long> m_localReplicas = ImmutableMap.<Integer, Long>builder().build();
    final long m_siteId;
    final long m_plannerSiteId;
    private final boolean m_isIV2Enabled;

    final Mailbox m_mailbox;

    private final QueueMonitor m_clientQueueMonitor = new QueueMonitor() {
        private final int MAX_QUEABLE = 33554432;

        private int m_queued = 0;

        @Override
        public boolean queue(int queued) {
            m_backpressureLock.lock();
            try {
                m_queued += queued;
                if (m_queued > MAX_QUEABLE) {
                    if (m_hasGlobalClientBackPressure || m_hasDTXNBackPressure) {
                        m_hasGlobalClientBackPressure = true;
                        //Guaranteed to already have reads disabled
                        return false;
                    }

                    m_hasGlobalClientBackPressure = true;
                    for (Connection c : m_connections) {
                        c.disableReadSelection();
                    }
                } else {
                    if (!m_hasGlobalClientBackPressure) {
                        return false;
                    }

                    if (m_hasGlobalClientBackPressure && !m_hasDTXNBackPressure) {
                        for (Connection c : m_connections) {
                            if (!c.writeStream().hadBackPressure()) {
                                /*
                                 * Also synchronize on the individual connection
                                 * so that enabling of read selection happens atomically
                                 * with the checking of client backpressure (client not reading responses)
                                 * in the write stream
                                 * so that this doesn't interleave incorrectly with
                                 * SimpleDTXNInitiator disabling read selection.
                                 */
                                synchronized (c) {
                                    if (!c.writeStream().hadBackPressure()) {
                                        c.enableReadSelection();
                                    }
                                }
                            }
                        }
                    }
                    m_hasGlobalClientBackPressure = false;
                }
            } finally {
                m_backpressureLock.unlock();
            }
            return false;
        }
    };

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
    private boolean m_hasDTXNBackPressure = false;

    // MAX_CONNECTIONS is updated to be (FD LIMIT - 300) after startup
    private final AtomicInteger MAX_CONNECTIONS = new AtomicInteger(800);
    private ScheduledFuture<?> m_maxConnectionUpdater;

    /**
     * Way too much data tied up sending responses to clients.
     * Wait until they receive data or have been booted.
     */
    private boolean m_hasGlobalClientBackPressure = false;
    private final boolean m_isConfiguredForHSQL;

    /** A port that accepts client connections */
    public class ClientAcceptor implements Runnable {
        private final int m_port;
        private final ServerSocketChannel m_serverSocket;
        private final VoltNetworkPool m_network;
        private volatile boolean m_running = true;
        private Thread m_thread = null;
        private final boolean m_isAdmin;

        /**
         * Used a cached thread pool to accept new connections.
         */
        private final ExecutorService m_executor =
                Executors.newCachedThreadPool(CoreUtils.getThreadFactory("Client authentication threads", "Client authenticator"));

        ClientAcceptor(int port, VoltNetworkPool network, boolean isAdmin)
        {
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
                    m_serverSocket.socket().bind(new InetSocketAddress(m_port));
                }
                catch (IOException e) {
                    hostLog.fatal("Client interface failed to bind to port " + m_port);
                    hostLog.fatal("IOException message: \"" + e.getMessage() + "\"");
                    MiscUtils.printPortsInUse(hostLog);
                    VoltDB.crashLocalVoltDB("Client interface failed to bind to port " + m_port, false, e);
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
            synchronized (this) {
                m_running = false;
                m_thread.interrupt();
            }
            m_thread.join();
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
                    if (m_numConnections.get() == MAX_CONNECTIONS.get()) {
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

                    m_executor.execute(new Runnable() {
                        @Override
                        public void run() {
                            if (socket != null) {
                                boolean success = false;
                                try {
                                    final InputHandler handler = authenticate(socket);
                                    if (handler != null) {
                                        socket.configureBlocking(false);
                                        if (handler instanceof ClientInputHandler) {
                                            socket.socket().setTcpNoDelay(true);
                                        }
                                        socket.socket().setKeepAlive(true);

                                        if (handler instanceof ClientInputHandler) {
                                            final Connection c = m_network.registerChannel(socket, handler, 0);
                                            /*
                                             * If IV2 is enabled the logic initially enabling read is
                                             * in the started method of the InputHandler
                                             */
                                            if (!m_isIV2Enabled) {
                                                m_backpressureLock.lock();
                                                try {
                                                    if (!m_hasDTXNBackPressure) {
                                                        c.enableReadSelection();
                                                    }
                                                    m_connections.add(c);
                                                } finally {
                                                    m_backpressureLock.unlock();
                                                }
                                            }
                                        } else {
                                            m_network.registerChannel(socket, handler, SelectionKey.OP_READ);
                                        }
                                        success = true;
                                    }
                                } catch (IOException e) {
                                    try {
                                        socket.close();
                                    } catch (IOException e1) {
                                        //Don't care connection is already lost anyways
                                    }
                                    if (m_running) {
                                        hostLog.warn("Exception authenticating and registering user in ClientAcceptor", e);
                                    }
                                } finally {
                                    if (!success) {
                                        m_numConnections.decrementAndGet();
                                    }
                                }
                            }
                        }
                    });
                } while (m_running);
            }  catch (IOException e) {
                if (m_running) {
                    hostLog.fatal("Exception in ClientAcceptor. The acceptor has died", e);
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
                        m_executor.awaitTermination( 1, TimeUnit.DAYS);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }

        /**
         * Attempt to authenticate the user associated with this socket connection
         * @param socket
         * @return AuthUser a set of user permissions or null if authentication fails
         * @throws IOException
         */
        private InputHandler
        authenticate(final SocketChannel socket) throws IOException
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
            ScheduledFuture<?> timeoutFuture = VoltDB.instance().scheduleWork(new Runnable() {
                                                    @Override
                                                    public void run() {
                                                        try {
                                                            socket.close();
                                                        } catch (IOException e) {
                                                            //Don't care
                                                        }
                                                    }
                                                }, 1600, 0, TimeUnit.MILLISECONDS);

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
                authLog.warn("Failure to authenticate connection(" + socket.socket().getRemoteSocketAddress() +
                             "): wire protocol violation (message length " + messageLength + " is negative).");
                //Send negative response
                responseBuffer.put(WIRE_PROTOCOL_FORMAT_ERROR).flip();
                socket.write(responseBuffer);
                socket.close();
                return null;
            }
            if (messageLength > ((1024 * 1024) * 2)) {
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
            message.get(password);

            CatalogContext context = m_catalogContext.get();

            /*
             * Don't use the auth system during recovery. Not safe to use
             * the node to initiate multi-partition txns during recovery
             */
            if (!VoltDB.instance().rejoining()) {
                /*
                 * Authenticate the user.
                 */
                boolean authenticated = context.authSystem.authenticate(username, password);

                if (!authenticated) {
                    authLog.warn("Failure to authenticate connection(" + socket.socket().getRemoteSocketAddress() +
                                 "): user " + username + " failed authentication.");
                    //Send negative response
                    responseBuffer.put(AUTHENTICATION_FAILURE).flip();
                    socket.write(responseBuffer);
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

            AuthSystem.AuthUser user = context.authSystem.getUser(username);

            /*
             * Create an input handler.
             */
            InputHandler handler = null;
            if (service.equalsIgnoreCase("database")) {
                handler =
                    new ClientInputHandler(
                            username,
                            socket.socket().getInetAddress().getHostName(),
                            m_isAdmin);
            }
            else {
                String strUser = "ANONYMOUS";
                if ((username != null) && (username.length() > 0)) strUser = username;

                // If no processor can handle this service, null is returned.
                String connectorClassName = ExportManager.instance().getConnectorForService(service);
                if (connectorClassName == null) {
                    //Send negative response
                    responseBuffer.put(EXPORT_DISABLED_REJECTION).flip();
                    socket.write(responseBuffer);
                    socket.close();
                    authLog.warn("Rejected user " + strUser +
                                 " attempting to use disabled or unconfigured service " +
                                 service + ".");
                    return null;
                }
                if (!user.authorizeConnector(connectorClassName)) {
                    //Send negative response
                    responseBuffer.put(AUTHENTICATION_FAILURE).flip();
                    socket.write(responseBuffer);
                    socket.close();
                    authLog.warn("Failure to authorize user " + strUser + " for service " + service + ".");
                    return null;
                }

                handler = ExportManager.instance().createInputHandler(service, m_isAdmin);
            }

            if (handler != null) {
                byte buildString[] = VoltDB.instance().getBuildString().getBytes("UTF-8");
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

            }
            else {
                authLog.warn("Failure to authenticate connection(" + socket.socket().getRemoteSocketAddress() +
                             "): user " + username + " failed authentication.");
                // Send negative response
                responseBuffer.put(AUTHENTICATION_FAILURE).flip();
                socket.write(responseBuffer);
                socket.close();
                return null;

            }
            return handler;
        }
    }

    /** A port that reads client procedure invocations and writes responses */
    public class ClientInputHandler extends VoltProtocolHandler implements AdmissionControlGroup.ACGMember {
        public static final int MAX_READ = 8192 * 4;

        private Connection m_connection;
        private final String m_hostname;
        private final boolean m_isAdmin;

        /**
         * Must use username to do a lookup via the auth system
         * rather then caching the AuthUser because the AuthUser
         * can be invalidated on catalog updates
         */
        private final String m_username;

        public ClientInputHandler(String username, String hostname,
                                  boolean isAdmin)
        {
            m_username = username.intern();
            m_hostname = hostname;
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
            if (m_isIV2Enabled) {
                m_cihm.put(c.connectionId(),
                           new ClientInterfaceHandleManager( m_isAdmin, c, m_acg.get()));
                m_acg.get().addMember(this);
                if (!m_acg.get().hasBackPressure()) {
                    c.enableReadSelection();
                }
                m_connections.add(c);
            }
        }

        @Override
        public void stopping(Connection c) {
            m_connections.remove(c);
        }

        @Override
        public void stopped(Connection c) {
            m_numConnections.decrementAndGet();
            m_initiator.removeConnectionStats(connectionId());
            /*
             * It's necessary to free all the resources held by the IV2 ACG tracking.
             * Outstanding requests may actually still be at large
             */
            if (m_isIV2Enabled) {
                ClientInterfaceHandleManager cihm = m_cihm.remove(connectionId());
                cihm.freeOutstandingTxns();
                cihm.m_acg.removeMember(this);
            }
        }

        /*
         * Runnables from returned by offBackPressure and onBackPressure are used
         * by the network when a specific connection signals backpressure
         * as opposed to the more global backpressure signaled by an ACG. The runnables
         * are only intended to enable/disable backpressure for the specific connection
         */
        @Override
        public Runnable offBackPressure() {
            if (m_isIV2Enabled) {
                return new Runnable() {
                    @Override
                    public void run() {
                        if (!m_acg.get().hasBackPressure()) {
                            m_connection.enableReadSelection();
                        }
                    }
                };
            }
            return new Runnable() {
                @Override
                public void run() {
                    /**
                     * Must synchronize to prevent a race between the DTXN backpressure starting
                     * and this attempt to reenable read selection (which should not occur
                     * if there is DTXN backpressure)
                     */
                    m_backpressureLock.lock();
                    try {
                        if (!m_hasDTXNBackPressure) {
                            m_connection.enableReadSelection();
                        }
                    } finally {
                        m_backpressureLock.unlock();
                    }
                }
            };
        }

        @Override
        public Runnable onBackPressure() {
            if (m_isIV2Enabled) {
                new Runnable() {
                    @Override
                    public void run() {
                        m_connection.disableReadSelection();
                    }
                };
            }
            return new Runnable() {
                @Override
                public void run() {
                    synchronized (m_connection) {
                        m_connection.disableReadSelection();
                    }
                }
            };
        }

        /*
         * Return a monitor for the number of outstanding bytes pending write to this network
         * connection
         */
        @Override
        public QueueMonitor writestreamMonitor() {
            if (m_isIV2Enabled) {
                return new QueueMonitor() {

                    @Override
                    public boolean queue(int bytes) {
                        return m_acg.get().queue(bytes);
                    }

                };
            }
            return m_clientQueueMonitor;
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
     * Invoked when DTXN backpressure starts
     *
     */
    public void onBackPressure() {
        log.trace("Had back pressure disabling read selection");
        m_backpressureLock.lock();
        try {
            m_hasDTXNBackPressure = true;
            for (final Connection c : m_connections) {
                c.disableReadSelection();
            }
        } finally {
            m_backpressureLock.unlock();
        }
    }

    /**
     * Invoked when DTXN backpressure stops
     *
     */
    public void offBackPressure() {
        log.trace("No more back pressure attempting to enable read selection");
        m_backpressureLock.lock();
        try {
            m_hasDTXNBackPressure = false;
            if (m_hasGlobalClientBackPressure) {
                return;
            }
            for (final Connection c : m_connections) {
                if (!c.writeStream().hadBackPressure()) {
                    /*
                     * Also synchronize on the individual connection
                     * so that enabling of read selection happens atomically
                     * with the checking of client backpressure (client not reading responses)
                     * in the write stream
                     * so that this doesn't interleave incorrectly with
                     * SimpleDTXNInitiator disabling read selection.
                     */
                    synchronized (c) {
                        if (!c.writeStream().hadBackPressure()) {
                            c.enableReadSelection();
                        }
                    }
                }
            }
        } finally {
            m_backpressureLock.unlock();
        }
    }

    // Wrap API to SimpleDtxnInitiator - mostly for the future
    public boolean createTransaction(
            final long connectionId,
            final String connectionHostname,
            final boolean adminConnection,
            final StoredProcedureInvocation invocation,
            final boolean isReadOnly,
            final boolean isSinglePartition,
            final boolean isEveryPartition,
            final int partitions[],
            final int numPartitions,
            final Object clientData,
            final int messageSize,
            final long now,
            final boolean allowMismatchedResults)
    {
        return createTransaction(
                connectionId,
                connectionHostname,
                adminConnection,
                Iv2InitiateTaskMessage.UNUSED_MP_TXNID,
                0, //unused timestammp
                invocation,
                isReadOnly,
                isSinglePartition,
                isEveryPartition,
                partitions,
                numPartitions,
                clientData,
                messageSize,
                now,
                allowMismatchedResults,
                false);  // is for replay.
    }

    // Wrap API to SimpleDtxnInitiator - mostly for the future
    public  boolean createTransaction(
            final long connectionId,
            final String connectionHostname,
            final boolean adminConnection,
            final long txnId,
            final long timestamp,
            final StoredProcedureInvocation invocation,
            final boolean isReadOnly,
            final boolean isSinglePartition,
            final boolean isEveryPartition,
            final int partitions[],
            final int numPartitions,
            final Object clientData,
            final int messageSize,
            final long now,
            final boolean allowMismatchedResults,
            final boolean isForReplay)
    {
        if (m_isIV2Enabled) {
            final ClientInterfaceHandleManager cihm = m_cihm.get(connectionId);

            Long initiatorHSId = null;
            boolean isShortCircuitRead = false;

            if (invocation.getType() == ProcedureInvocationType.REPLICATED)
            {
                int partitionId;
                if (isSinglePartition) {
                    partitionId = partitions[0];
                } else {
                    partitionId = MpInitiator.MP_INIT_PID;
                }

                Long lastTxnId = m_partitionTxnIds.get(partitionId);
                if (lastTxnId != null) {
                    /*
                     * Ning - @LoadSinglepartTable and @LoadMultipartTable
                     * always have the same txnId which is the txnId of the
                     * snapshot.
                     */
                    if (!(invocation.getProcName().equalsIgnoreCase("@LoadSinglepartitionTable") ||
                            invocation.getProcName().equalsIgnoreCase("@LoadMultipartitionTable")) &&
                            invocation.getOriginalTxnId() <= lastTxnId)
                    {
                        hostLog.debug("Dropping duplicate replicated transaction " + invocation.getProcName() + ", txnid: " +
                                invocation.getOriginalTxnId() + ", last seen: " + lastTxnId);
                        return false;
                    }
                }
                m_partitionTxnIds.put(partitionId, invocation.getOriginalTxnId());
            }

            /*
             * If this is a read only single part, check if there is a local replica,
             * if there is, send it to the replica as a short circuit read
             */
            if (isSinglePartition && !isEveryPartition) {
                if (isReadOnly) {
                    initiatorHSId = m_localReplicas.get(partitions[0]);
                }
                if (initiatorHSId != null) {
                    isShortCircuitRead = true;
                } else {
                    initiatorHSId = m_iv2Masters.get(partitions[0]);
                }
            }
            else {
                //Multi-part transactions go to the multi-part coordinator
                initiatorHSId = m_cartographer.getHSIdForMultiPartitionInitiator();
            }

            if (initiatorHSId == null) {
                hostLog.error("Failed to find master initiator for partition: "
                        + Integer.toString(partitions[0]) + ". Transaction not initiated.");
                return false;
            }

            long handle = cihm.getHandle(isSinglePartition, partitions[0], invocation.getClientHandle(),
                    messageSize, now, invocation.getProcName(), initiatorHSId, isReadOnly, isShortCircuitRead);

            Iv2InitiateTaskMessage workRequest =
                new Iv2InitiateTaskMessage(m_siteId,
                        initiatorHSId,
                        Iv2InitiateTaskMessage.UNUSED_TRUNC_HANDLE,
                        txnId,
                        timestamp,
                        isReadOnly,
                        isSinglePartition,
                        invocation,
                        handle,
                        connectionId,
                        isForReplay);

            Iv2Trace.logCreateTransaction(workRequest);
            m_mailbox.send(initiatorHSId, workRequest);
            return true;
        } else {
            return m_initiator.createTransaction(connectionId,
                                                 connectionHostname,
                                                 adminConnection,
                                                 invocation,
                                                 isReadOnly,
                                                 isSinglePartition,
                                                 isEveryPartition,
                                                 partitions,
                                                 numPartitions,
                                                 clientData,
                                                 messageSize,
                                                 now,
                                                 allowMismatchedResults);
        }
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
            SimpleDtxnInitiator initiator,
            Cartographer cartographer,
            int partitionCount,
            int port,
            int adminPort,
            long timestampTestingSalt) throws Exception {

        // create a list of all partitions
        int[] allPartitions = new int[partitionCount];
        int index = 0;
        for (Integer partition : VoltDB.instance().getSiteTracker().m_partitionsToSitesImmutable.keySet()) {
            allPartitions[index++] = partition;
        }

        /*
         * Construct the runnables so they have access to the list of connections
         */
        final ClientInterface ci = new ClientInterface(
           port, adminPort, context, messenger, replicationRole, initiator, cartographer, allPartitions);

        initiator.setClientInterface(ci);
        return ci;
    }

    ClientInterface(int port, int adminPort, CatalogContext context, HostMessenger messenger,
                    ReplicationRole replicationRole, TransactionInitiator initiator,
                    Cartographer cartographer, int[] allPartitions) throws Exception
    {
        m_catalogContext.set(context);
        m_initiator = initiator;
        m_cartographer = cartographer;

        // pre-allocate single partition array
        m_allPartitions = allPartitions;
        m_acceptor = new ClientAcceptor(port, messenger.getNetwork(), false);
        m_adminAcceptor = null;
        m_adminAcceptor = new ClientAcceptor(adminPort, messenger.getNetwork(), true);
        registerPolicies(replicationRole);

        m_mailbox = new LocalMailbox(messenger,  messenger.getHSIdForLocalSite(HostMessenger.CLIENT_INTERFACE_SITE_ID)) {
            LinkedBlockingQueue<VoltMessage> m_d = new LinkedBlockingQueue<VoltMessage>();
            @Override
            public void deliver(final VoltMessage message) {
                if (m_isIV2Enabled) {
                    if (message instanceof InitiateResponseMessage) {
                        // forward response; copy is annoying. want slice of response.
                        final InitiateResponseMessage response = (InitiateResponseMessage)message;
                        Iv2Trace.logFinishTransaction(response, m_mailbox.getHSId());
                        final ClientInterfaceHandleManager cihm = m_cihm.get(response.getClientConnectionId());
                        //Can be null on hangup
                        if (cihm != null) {
                            //Pass it to the network thread like a ninja
                            //Only the network can use the CIHM
                            cihm.connection.writeStream().enqueue(
                                    new DeferredSerialization() {

                                        @Override
                                        public ByteBuffer[] serialize()
                                                throws IOException {
                                            ClientResponseImpl clientResponse = response.getClientResponseData();
                                            ClientInterfaceHandleManager.Iv2InFlight clientData =
                                                    cihm.findHandle(response.getClientInterfaceHandle());
                                            if (clientData == null) {
                                                return new ByteBuffer[] {};
                                            }
                                            final long now = System.currentTimeMillis();
                                            final int delta = (int)(now - clientData.m_creationTime);

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
                                            clientResponse.setClusterRoundtrip(delta);
                                            clientResponse.setHash(null); // not part of wire protocol

                                            ByteBuffer results =
                                                    ByteBuffer.allocate(
                                                            clientResponse.getSerializedSize() + 4);
                                            results.putInt(results.capacity() - 4);
                                            clientResponse.flattenToBuffer(results);
                                            return new ByteBuffer[] { results };
                                        }

                                        @Override
                                        public void cancel() {
                                        }

                            });
                        }
                    } else if (message instanceof BinaryPayloadMessage) {
                        handlePartitionFailOver((BinaryPayloadMessage)message);
                    } else {
                        m_d.offer(message);
                    }
                } else {
                    m_d.offer(message);
                }
            }

            @Override
            public VoltMessage recv() {
                return m_d.poll();
            }
        };
        m_isIV2Enabled = VoltDB.instance().isIV2Enabled();
        messenger.createMailbox(m_mailbox.getHSId(), m_mailbox);
        m_plannerSiteId = messenger.getHSIdForLocalSite(HostMessenger.ASYNC_COMPILER_SITE_ID);
        m_zk = messenger.getZK();
        registerMailbox(m_zk);
        m_siteId = m_mailbox.getHSId();
        m_iv2Masters = new LeaderCache(m_zk, VoltZK.iv2masters);
        m_iv2Masters.start(true);
        m_isConfiguredForHSQL = (VoltDB.instance().getBackendTargetType() == BackendTarget.HSQLDB_BACKEND);
    }

    private void handlePartitionFailOver(BinaryPayloadMessage message) {
        try {
            JSONObject jsObj = new JSONObject(new String(message.m_payload, "UTF-8"));
            final int partitionId = jsObj.getInt(BaseInitiator.JSON_PARTITION_ID);
            final long initiatorHSId = jsObj.getLong(BaseInitiator.JSON_INITIATOR_HSID);
            for (final Connection c : m_connections) {
                c.queueTask(new Runnable() {
                    @Override
                    public void run() {
                        failOverConnection(partitionId, initiatorHSId, c);
                    }
                });
            }
            failOverConnection(partitionId, initiatorHSId, m_snapshotDaemonAdapter);
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
     * Publishes the HSId of this execution site to ZK
     * @param zk
     * @param partitionId
     * @throws Exception
     */
    private void registerMailbox(ZooKeeper zk) throws Exception {
        MailboxNodeContent mnc = new MailboxNodeContent(m_mailbox.getHSId(), null);
        VoltDB.instance().getMailboxPublisher().registerMailbox(MailboxType.ClientInterface, mnc);
        VoltDB.instance().getMailboxPublisher().publish(zk);
    }

    private void registerPolicies(ReplicationRole replicationRole) {
        registerPolicy(new InvocationPermissionPolicy(true));
        registerPolicy(new ParameterDeserializationPolicy(true));
        registerPolicy(new ReplicaInvocationAcceptancePolicy(replicationRole == ReplicationRole.REPLICA));

        registerPolicy("@AdHoc", new AdHocAcceptancePolicy(true));
        registerPolicy("@UpdateApplicationCatalog", new UpdateCatalogAcceptancePolicy(true));
    }

    private void registerPolicy(InvocationAcceptancePolicy policy) {
        List<InvocationAcceptancePolicy> policies = m_policies.get(null);
        if (policies == null) {
            policies = new ArrayList<InvocationAcceptancePolicy>();
            m_policies.put(null, policies);
        }
        policies.add(policy);
    }

    private void registerPolicy(String procName, InvocationAcceptancePolicy policy) {
        List<InvocationAcceptancePolicy> policies = m_policies.get(procName);
        if (policies == null) {
            policies = new ArrayList<InvocationAcceptancePolicy>();
            m_policies.put(procName, policies);
        }
        policies.add(policy);
    }

    /**
     * Check the procedure invocation against a set of policies to see if it
     * should be rejected.
     *
     * @param name The procedure name, null for generic policies.
     * @return ClientResponseImpl on error or null if okay.
     */
    private ClientResponseImpl checkPolicies(String name, AuthSystem.AuthUser user,
                                  final StoredProcedureInvocation task,
                                  final Procedure catProc, Config sysProc) {
        List<InvocationAcceptancePolicy> policies = m_policies.get(name);
        ClientResponseImpl error = null;
        if (policies != null) {
            for (InvocationAcceptancePolicy policy : policies) {
                if (catProc != null) {
                    if ((error = policy.shouldAccept(user, task, catProc)) != null) {
                        return error;
                    }
                } else {
                    if ((error = policy.shouldAccept(user, task, sysProc)) != null) {
                        return error;
                    }
                }
            }
        }
        return null;
    }

    /**
     * Called when the replication role of the cluster changes.
     * @param role
     */
    public void setReplicationRole(ReplicationRole role) {
        List<InvocationAcceptancePolicy> policies = m_policies.get(null);
        if (policies != null) {
            for (InvocationAcceptancePolicy policy : policies) {
                if (policy instanceof ReplicaInvocationAcceptancePolicy) {
                    policy.setMode(role == ReplicationRole.REPLICA);
                }
            }
        }
    }

    /**
     * Initializes the snapshot daemon so that it's ready to take snapshots
     */
    public void initializeSnapshotDaemon(ZooKeeper zk, GlobalServiceElector gse) {
        m_snapshotDaemon.init(this, zk, new Runnable() {
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
        if (VoltDB.instance().getSiteTracker().isFirstHost() &&
            schedule != null && schedule.getEnabled())
        {
            Future<Void> future = m_snapshotDaemon.makeActive(schedule);
            try {
                future.get();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                throw new RuntimeException(e.getCause());
            }
        } else {
            m_snapshotDaemon.makeInactive();
        }
    }

    /**
     * Set the flag that tells this client interface to update its
     * catalog when it's threadsafe.
     */
    public void notifyOfCatalogUpdate() {
        m_shouldUpdateCatalog.set(true);
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

    private void processExplainPlannedStmtBatch(  AdHocPlannedStmtBatch planBatch ) {
            final Connection c = (Connection)planBatch.clientData;
            Database db = m_catalogContext.get().database;
            int size = planBatch.getPlannedStatementCount();

            List<byte[]> aggByteArray = new ArrayList<byte[]>( size );
            for (AdHocPlannedStatement plannedStatement : planBatch.plannedStatements ) {
                aggByteArray.add(plannedStatement.core.aggregatorFragment);
            }

            List<byte[]> collByteArray = new ArrayList<byte[]>( size );
            for (AdHocPlannedStatement plannedStatement : planBatch.plannedStatements ) {
                collByteArray.add(plannedStatement.core.collectorFragment);
            }

            VoltTable[] vt = new VoltTable[ size ];

            for( int i = 0; i<size; i++ ) {
                byte[] aggByte = aggByteArray.get(i);
                byte[] collByte = collByteArray.get(i);
                if( collByte == null ) {
                    //signle partition query plan
                    String plan = new String( aggByte, VoltDB.UTF8ENCODING);
                    PlanNodeTree pnt = new PlanNodeTree();
                    try {
                        JSONObject jobj = new JSONObject( plan );
                        JSONArray jarray =  jobj.getJSONArray(PlanNodeTree.Members.PLAN_NODES.name());
                        pnt.loadFromJSONArray(jarray, db);
                        String str = pnt.getRootPlanNode().toExplainPlanString();
                        vt[i] = new VoltTable(new VoltTable.ColumnInfo( "EXECUTION_PLAN", VoltType.STRING));
                        vt[i].addRow(str);
                    } catch (JSONException e) {
                        System.out.println(e.getMessage());
                    }
                }
                else {
                    //multi-partition query plan
                    String aggplan = new String( aggByte, VoltDB.UTF8ENCODING);
                    String collplan = new String( collByte, VoltDB.UTF8ENCODING);
                    PlanNodeTree pnt = new PlanNodeTree();
                    PlanNodeTree collpnt = new PlanNodeTree();
                    try {
                        JSONObject jobj = new JSONObject( aggplan );
                        JSONArray jarray =  jobj.getJSONArray(PlanNodeTree.Members.PLAN_NODES.name());
                        pnt.loadFromJSONArray(jarray, db);
                        //reattach plan fragments
                        jobj = new JSONObject( collplan );
                        jarray =  jobj.getJSONArray(PlanNodeTree.Members.PLAN_NODES.name());
                        collpnt.loadFromJSONArray(jarray, db);
                        assert( collpnt.getRootPlanNode() instanceof SendPlanNode);
                        pnt.getRootPlanNode().reattachFragment( (SendPlanNode) collpnt.getRootPlanNode() );

                        String str = pnt.getRootPlanNode().toExplainPlanString();
                        vt[i] = new VoltTable(new VoltTable.ColumnInfo( "EXECUTION_PLAN", VoltType.STRING));
                        vt[i].addRow(str);
                    } catch (JSONException e) {
                        System.out.println(e.getMessage());
                    }
                }
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

         //do not cache the plans for explainAdhoc
    //        planBatch.clientData = null;
    //        for (int index = 0; index < planBatch.getPlannedStatementCount(); index++) {
    //            m_adhocCache.put(planBatch.getPlannedStatement(index));
    //        }
        }

    //go to catolog and fetch all explain plan of queries in the procedure
    ClientResponseImpl dispatchExplainProcedure(StoredProcedureInvocation task, ClientInputHandler handler, Connection ccxn) {
        ParameterSet params = task.getParams();
        //String procs = (String) params.toArray()[0];
        List<String> procNames = MiscUtils.splitSQLStatements( (String)params.toArray()[0]);
        int size = procNames.size();
        VoltTable[] vt = new VoltTable[ size ];
        for( int i=0; i<size; i++ ) {
            String procName = procNames.get(i);

            Procedure proc = m_catalogContext.get().procedures.get(procName);
            if(proc == null) {
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
                vt[i].addRow( stmt.getSqltext()+"\n", Encoder.hexDecodeToString( stmt.getExplainplan() ) );
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

    ClientResponseImpl dispatchAdHoc(StoredProcedureInvocation task, ClientInputHandler handler, Connection ccxn, boolean isExplain) {
        ParameterSet params = task.getParams();
        String sql = (String) params.toArray()[0];

        // get the partition param if it exists
        // null means MP-txn
        Object partitionParam = null;
        if (params.toArray().length > 1) {
            if (params.toArray()[1] == null) {
                // nulls map to zero
                partitionParam = new Long(0);
                // skip actual null value because it means MP txn
            }
            else {
                partitionParam = params.toArray()[1];
            }
        }

        List<String> sqlStatements = MiscUtils.splitSQLStatements(sql);

        AdHocPlannerWork ahpw = new AdHocPlannerWork(
                m_siteId,
                false, task.clientHandle, handler.connectionId(),
                handler.m_hostname, handler.isAdmin(), ccxn,
                sql, sqlStatements, partitionParam, null, false, true,
                task.type, task.originalTxnId, task.originalTs,
                m_adhocCompletionHandler);
        if( isExplain ){
            ahpw.setIsExplainWork();
        }
        LocalObjectMessage work = new LocalObjectMessage( ahpw );

        m_mailbox.send(m_plannerSiteId, work);
        return null;
    }

    ClientResponseImpl dispatchUpdateApplicationCatalog(StoredProcedureInvocation task,
            ClientInputHandler handler, Connection ccxn)
    {
        ParameterSet params = task.getParams();
        byte[] catalogBytes = null;
        if (params.toArray()[0] instanceof String) {
            catalogBytes = Encoder.hexDecode((String) params.toArray()[0]);
        } else if (params.toArray()[0] instanceof byte[]) {
            catalogBytes = (byte[]) params.toArray()[0];
        } else {
            // findbugs triggers a NPE alert here... and the
            // policy check is pretty far away from here.
            // assert and satisfy findbugs, and the casual reader.
            assert false : "Expected to catch invalid parameters in UpdateCatalogAcceptancePolicy.";
            return new ClientResponseImpl(ClientResponseImpl.GRACEFUL_FAILURE,
                    new VoltTable[0], "Failed to process UpdateApplicationCatalog request." +
                    " Catalog content must be passed as string or byte[].",
                    task.clientHandle);
        }
        String deploymentString = (String) params.toArray()[1];
        LocalObjectMessage work = new LocalObjectMessage(
                new CatalogChangeWork(
                    m_siteId,
                    task.clientHandle, handler.connectionId(), handler.m_hostname,
                    handler.isAdmin(), ccxn, catalogBytes, deploymentString,
                    m_adhocCompletionHandler));

        m_mailbox.send(m_plannerSiteId, work);
        return null;
    }

    ClientResponseImpl dispatchLoadSinglepartitionTable(ByteBuffer buf,
            StoredProcedureInvocation task, ClientInputHandler handler, Connection ccxn)
    {
        int[] involvedPartitions = null;
        // break out the Hashinator and calculate the appropriate partition
        try {
            CatalogMap<Table> tables = m_catalogContext.get().database.getTables();
            Object valueToHash =
                LoadSinglepartitionTable.
                    partitionValueFromInvocation(tables, task);
            involvedPartitions =
                new int[] { TheHashinator.hashToPartition(valueToHash) };
        }
        catch (Exception e) {
            authLog.warn(e.getMessage());
            return new ClientResponseImpl(ClientResponseImpl.UNEXPECTED_FAILURE,
                    new VoltTable[0], e.getMessage(), task.clientHandle);
        }
        assert(involvedPartitions != null);
        // XXX-ZOMG This really should pass in the SystemProcedureCatalog.Config object
        // and read these settings out of it rather than hardwiring them here.
        createTransaction(handler.connectionId(), handler.m_hostname,
                handler.isAdmin(),
                task,
                false,      // read only
                true,       // single partition
                false,      // every site
                involvedPartitions, involvedPartitions.length,
                ccxn, buf.capacity(),
                System.currentTimeMillis(),
                false);     // allow mismatched results
        return null;
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
            sendSentinel(txnId, partition, false);
        }
    }

    /**
     * Send a multipart sentinel to the specified partition. This comes from the
     * DR agent in prepare of a multipart transaction.
     *
     * @param buf
     * @param invocation
     * @return
     */
    ClientResponseImpl dispatchSendSentinel(ByteBuffer buf,
                                            StoredProcedureInvocation invocation)
    {
        /*
         * Sentinels will be deduped by ReplaySequencer. They don't advance the
         * last replayed txnIds.
         */

        // First parameter is the partition ID
        sendSentinel(invocation.getOriginalTxnId(),
                     (Integer) invocation.getParameterAtIndex(0), false);

        ClientResponseImpl response =
                new ClientResponseImpl(ClientResponseImpl.UNEXPECTED_FAILURE,
                                       new VoltTable[0],
                                       ClientResponseImpl.DUPE_TRANSACTION,
                                       invocation.clientHandle);
        return response;
    }

    ClientResponseImpl dispatchStatistics(Config sysProc, ByteBuffer buf, StoredProcedureInvocation task,
            ClientInputHandler handler, Connection ccxn) {
        ParameterSet params = task.getParams();
        // dispatch selectors that do not us the @Statistics system procedure
        if ((params.toArray().length != 0)) {
            String selector = (String)params.toArray()[0];
            if (selector.equals("DR") || selector.equals("TOPO")) {
               try {
                   VoltDB.instance().getStatsAgent().collectStats(ccxn, task.clientHandle, selector);
                   return null;
               } catch (Exception e) {
                   return errorResponse( ccxn, task.clientHandle, ClientResponse.UNEXPECTED_FAILURE, null, e, true);
               }
           }
        }
        int[] involvedPartitions = m_allPartitions;
        createTransaction(handler.connectionId(), handler.m_hostname,
                handler.isAdmin(),
                task,
                sysProc.getReadonly(),
                sysProc.getSinglepartition(),
                sysProc.getEverysite(),
                involvedPartitions, involvedPartitions.length,
                ccxn, buf.capacity(),
                System.currentTimeMillis(),
                false);
        return null;
    }

    /**
     * Allows delayed transaction for @Promote promotion so that it can optionally
     * happen after a truncation snapshot. (ENG-3880)
     */
    private class Promoter
    {
        final Config m_sysProc;
        final StoredProcedureInvocation m_task;
        final org.voltdb.catalog.CommandLog m_commandLog;
        final Connection m_ccxn;
        final boolean m_isAdmin;
        final String m_hostName;
        final long m_connectionId;
        final int m_messageSize;

        // Constructor
        Promoter(final Config sysProc,
                 final StoredProcedureInvocation task,
                 final org.voltdb.catalog.CommandLog commandLog,
                 final Connection ccxn,
                 final boolean isAdmin,
                 final String hostName,
                 final long connectionId,
                 final int messageSize)
        {
            m_sysProc = sysProc;
            m_task = task.getShallowCopy();
            m_task.setProcName("@PromoteReplicaStatus");
            m_commandLog = commandLog;
            m_ccxn = ccxn;
            m_isAdmin = isAdmin;
            m_hostName = hostName;
            m_connectionId = connectionId;
            m_messageSize = messageSize;
        }

        // Promote the replica.
        void promote() {
            m_task.procName = "@PromoteReplicaStatus";
            int[] involvedPartitions = m_allPartitions;
            createTransaction(m_connectionId,
                              m_hostName,
                              m_isAdmin,
                              m_task,
                              m_sysProc.getReadonly(),
                              m_sysProc.getSinglepartition(),
                              m_sysProc.getEverysite(),
                              involvedPartitions,
                              involvedPartitions.length,
                              m_ccxn,
                              m_messageSize,
                              System.currentTimeMillis(),
                              false);
        }

        // Trigger a truncation snapshot and then promote the replica.
        void truncateAndPromote() {
            try {
                // Use the current time as an identifier (nonce) that can be
                // recognized below by the monitor so that the promote doesn't
                // happen until our snapshot completes.
                final String reqId = java.util.UUID.randomUUID().toString();
                SnapshotCompletionMonitor completionMonitor =
                        VoltDB.instance().getSnapshotCompletionMonitor();
                completionMonitor.addInterest(new SnapshotCompletionInterest() {
                    @Override
                    public CountDownLatch snapshotCompleted(SnapshotCompletionEvent event) {
                        // Is this our snapshot?
                        if (event.truncationSnapshot && reqId.equals(event.requestId)) {
                            promote();
                        }
                        return null;
                    }
                });
                m_zk.create(VoltZK.request_truncation_snapshot, reqId.getBytes(),
                        Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            catch (Exception e) {
                VoltDB.crashGlobalVoltDB("ZK truncation snapshot request failed", false, e);
            }
        }
    }

    ClientResponseImpl dispatchPromote(Config sysProc,
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

        // ENG-3880 Perform a truncation snapshot so that transaction IDs and
        // timestamps generated locally for command logging aren't used for durability.
        // The host with the lowest host ID initiates the truncation snapshot.
        org.voltdb.catalog.CommandLog logConfig = m_catalogContext.get().cluster.getLogconfig().get("log");
        Promoter promoter = new Promoter(sysProc, task, logConfig, ccxn, handler.isAdmin(),
                                         handler.m_hostname, handler.connectionId(), buf.capacity());
        // This only happens on one node so we don't need to pick a leader.
        if (logConfig.getEnabled()) {
            promoter.truncateAndPromote();
        }
        else {
            promoter.promote();
        }
        return null;
    }

    /**
     *
     * @param port
     * * return True if an error was generated and needs to be returned to the client
     */
    final ClientResponseImpl handleRead(ByteBuffer buf, ClientInputHandler handler, Connection ccxn) throws IOException {
        final long now = System.currentTimeMillis();
        final FastDeserializer fds = new FastDeserializer(buf);
        final StoredProcedureInvocation task = fds.readObject(StoredProcedureInvocation.class);
        ClientResponseImpl error = null;

        // Check for admin mode restrictions before proceeding any further
        VoltDBInterface instance = VoltDB.instance();
        if (instance.getMode() == OperationMode.PAUSED && !handler.isAdmin())
        {
            return new ClientResponseImpl(ClientResponseImpl.SERVER_UNAVAILABLE,
                    new VoltTable[0], "Server is currently unavailable; try again later",
                    task.clientHandle);
        }

        // ping just responds as fast as possible to show the connection is alive
        // nb: ping is not a real procedure, so this is checked before other "sysprocs"
        if (task.procName.equals("@Ping")) {
            return new ClientResponseImpl(ClientResponseImpl.SUCCESS, new VoltTable[0], "", task.clientHandle);
        }

        // Deserialize the client's request and map to a catalog stored procedure
        final CatalogContext catalogContext = m_catalogContext.get();
        AuthSystem.AuthUser user = catalogContext.authSystem.getUser(handler.m_username);
        final Procedure catProc = catalogContext.procedures.get(task.procName);
        Config sysProc = SystemProcedureCatalog.listing.get(task.procName);

        if (sysProc == null ) {
            if( task.procName.equals("@AdHoc") ){
                // Map @AdHoc... to @AdHoc_RW_MP for validation. In the future if security is
                // configured differently for @AdHoc... variants this code will have to
                // change in order to use the proper variant based on whether the work
                // is single or multi partition and read-only or read-write.
                sysProc = SystemProcedureCatalog.listing.get("@AdHoc_RW_MP");
                assert(sysProc != null);
            }
            else if( task.procName.equals("@Explain") ){
                return dispatchAdHoc(task, handler, ccxn, true );
            }
            else if(task.procName.equals("@ExplainProc")) {
                return dispatchExplainProcedure(task, handler, ccxn);
            }
            else if (task.procName.equals("@SendSentinel")) {
                return dispatchSendSentinel(buf, task);
            }
            else if (task.procName.equals("@Promote")) {
                // Map @Promote to @PromoteReplicaState.
                sysProc = SystemProcedureCatalog.listing.get("@PromoteReplicaStatus");
                assert(sysProc != null);
            }
        }

        if (user == null) {
            authLog.info("User " + handler.m_username + " has been removed from the system via a catalog update");
            return new ClientResponseImpl(ClientResponseImpl.UNEXPECTED_FAILURE,
                    new VoltTable[0], "User " + handler.m_username +
                    " has been removed from the system via a catalog update",
                    task.clientHandle);
        }

        if (catProc == null && sysProc == null) {
            String errorMessage = "Procedure " + task.procName + " was not found";
            authLog.l7dlog( Level.WARN, LogKeys.auth_ClientInterface_ProcedureNotFound.name(), new Object[] { task.procName }, null);
            return new ClientResponseImpl(
                    ClientResponseImpl.UNEXPECTED_FAILURE,
                    new VoltTable[0], errorMessage, task.clientHandle);
        }

        // Check procedure policies
        error = checkPolicies(null, user, task, catProc, sysProc);
        if (error != null) {
            return error;
        }

        error = checkPolicies(task.procName, user, task, catProc, sysProc);
        if (error != null) {
            return error;
        }

        if (sysProc != null) {
            // these have helpers that do all the work...
            if (task.procName.equals("@AdHoc")) {
                return dispatchAdHoc(task, handler, ccxn, false);
            } else if (task.procName.equals("@UpdateApplicationCatalog")) {
                return dispatchUpdateApplicationCatalog(task, handler, ccxn);
            } else if (task.procName.equals("@LoadSinglepartitionTable")) {
                return dispatchLoadSinglepartitionTable(buf, task, handler, ccxn);
            } else if (task.procName.equals("@LoadMultipartitionTable")) {
                /*
                 * For IV2 DR: This will generate a sentinel for each partition,
                 * but doesn't initiate the invocation. It will fall through to
                 * the shared dispatch of sysprocs.
                 */
                if (VoltDB.instance().isIV2Enabled() &&
                        task.getType() == ProcedureInvocationType.REPLICATED) {
                    sendSentinelsToAllPartitions(task.getOriginalTxnId());
                }
            } else if (task.procName.equals("@SnapshotSave")) {
                m_snapshotDaemon.requestUserSnapshot(task, ccxn);
                return null;
            } else if (task.procName.equals("@Statistics")) {
                return dispatchStatistics(sysProc, buf, task, handler, ccxn);
            } else if (task.procName.equals("@Promote")) {
                return dispatchPromote(sysProc, buf, task, handler, ccxn);
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
            else if (task.procName.equals("@SystemInformation")) {
                ParameterSet params = task.getParams();
                // hacky: support old @SystemInformation behavior by
                // filling in a missing selector to get the overview key/value info
                if (params.toArray().length == 0) {
                    task.setParams("OVERVIEW");
                }
            }

            // the shared dispatch for sysprocs
            int[] involvedPartitions = m_allPartitions;
            if (sysProc.getSinglepartition()) {
                //Fix a bug where SystemCatalog was sent to all partitions
                //and catalog changes caused result mismatches
                //Pick a random partition to be the source of the catalog info
                involvedPartitions = new int[] { new java.util.Random().nextInt(involvedPartitions.length) };
            }

            createTransaction(handler.connectionId(), handler.m_hostname,
                    handler.isAdmin(),
                    task,
                    sysProc.getReadonly(),
                    sysProc.getSinglepartition(),
                    sysProc.getEverysite(),
                    involvedPartitions, involvedPartitions.length,
                    ccxn, buf.capacity(),
                    now,
                    false);

        }

        // dispatch a user procedure
        if (catProc != null) {
            int[] involvedPartitions = null;
            if (catProc.getSinglepartition() == false) {
                involvedPartitions = m_allPartitions;
            }
            else {
                // break out the Hashinator and calculate the appropriate partition
                try {
                    involvedPartitions = new int[] {
                                getPartitionForProcedure(
                                        catProc.getPartitionparameter(),
                                        catProc.getPartitioncolumn().getType(),
                                        task)
                            };
                }
                catch (RuntimeException e) {
                    // unable to hash to a site, return an error
                    String errorMessage = "Error sending procedure "
                        + task.procName + " to the correct partition. Make sure parameter values are correct.";
                    authLog.l7dlog( Level.WARN,
                            LogKeys.host_ClientInterface_unableToRouteSinglePartitionInvocation.name(),
                            new Object[] { task.procName }, null);
                    return new ClientResponseImpl(ClientResponseImpl.UNEXPECTED_FAILURE,
                            new VoltTable[0], errorMessage, task.clientHandle);
                }
                catch (Exception e) {
                    authLog.l7dlog( Level.WARN,
                            LogKeys.host_ClientInterface_unableToRouteSinglePartitionInvocation.name(),
                            new Object[] { task.procName }, null);
                    return new ClientResponseImpl(ClientResponseImpl.UNEXPECTED_FAILURE,
                            new VoltTable[0], e.getMessage(), task.clientHandle);
                }
            }
            boolean allowMismatchedResults = catProc.getReadonly() && isProcedureNonDeterministic(catProc);
            boolean success =
                createTransaction(handler.connectionId(), handler.m_hostname,
                        handler.isAdmin(),
                        task,
                        catProc.getReadonly(),
                        catProc.getSinglepartition(),
                        catProc.getEverysite(),
                        involvedPartitions, involvedPartitions.length,
                        ccxn, buf.capacity(),
                        now,
                        allowMismatchedResults);
            if (!success) {
                // HACK: this return is for the DR agent so that it
                // will move along on duplicate replicated transactions
                // reported by the slave cluster.  We report "SUCCESS"
                // to keep the agent from choking.  ENG-2334
                return new ClientResponseImpl(ClientResponseImpl.UNEXPECTED_FAILURE,
                        new VoltTable[0],
                        ClientResponseImpl.DUPE_TRANSACTION,
                        task.clientHandle);
            }
        }
        return null;
    }

    /**
     * Determine if a procedure is non-deterministic by examining all its statements.
     *
     * @param proc  catalog procedure
     * @return  true if it has any non-deterministic statements
     */
    static boolean isProcedureNonDeterministic(Procedure proc) {
        boolean isNonDeterministic = false;
        CatalogMap<Statement> stmts = proc.getStatements();
        if (stmts != null) {
            for (Statement stmt : stmts) {
                if (!stmt.getIscontentdeterministic() || !stmt.getIsorderdeterministic()) {
                    isNonDeterministic = true;
                    break;
                }
            }
        }
        return isNonDeterministic;
    }

    void createAdHocTransaction(final AdHocPlannedStmtBatch plannedStmtBatch) {
        // create the execution site task
        StoredProcedureInvocation task = new StoredProcedureInvocation();
        // DR stuff
        task.type = plannedStmtBatch.type;
        task.originalTxnId = plannedStmtBatch.originalTxnId;
        task.originalTs = plannedStmtBatch.originalTs;
        // pick the sysproc based on the presence of partition info
        // HSQL does not specifically implement AdHoc SP -- instead, use its always-SP implementation of AdHoc
        boolean isSinglePartition = plannedStmtBatch.isSinglePartitionCompatible() || m_isConfiguredForHSQL;
        int partitions[] = null;

        if (isSinglePartition) {
            if (plannedStmtBatch.isReadOnly()) {
                task.procName = "@AdHoc_RO_SP";
            }
            else {
                task.procName = "@AdHoc_RW_SP";
            }
            partitions = new int[] { TheHashinator.hashToPartition(plannedStmtBatch.partitionParam) };
        }
        else {
            if (plannedStmtBatch.isReadOnly()) {
                task.procName = "@AdHoc_RO_MP";
            }
            else {
                task.procName = "@AdHoc_RW_MP";
            }
            partitions = m_allPartitions;
        }

        // Set up the parameters.
        ByteBuffer buf = ByteBuffer.allocate(plannedStmtBatch.getPlanArraySerializedSize());
        try {
            plannedStmtBatch.flattenPlanArrayToBuffer(buf);
        }
        catch (Exception e) {
            VoltDB.crashLocalVoltDB(e.getMessage(), true, e);
        }
        assert(buf.hasArray());
        task.setParams(buf.array());
        task.clientHandle = plannedStmtBatch.clientHandle;

        /*
         * Round trip the invocation to initialize it for command logging
         */
        FastSerializer fs = new FastSerializer();
        int serializedSize = 0;
        try {
            fs.writeObject(task);
            ByteBuffer source = fs.getBuffer();
            ByteBuffer copy = ByteBuffer.allocate(source.remaining());
            serializedSize = copy.capacity();
            copy.put(source);
            copy.flip();
            FastDeserializer fds = new FastDeserializer(copy);
            task = new StoredProcedureInvocation();
            task.readExternal(fds);
        } catch (Exception e) {
            VoltDB.crashLocalVoltDB(e.getMessage(), true, e);
        }

        // ENG-3288 - Non-deterministic read-only queries tolerate mismatched results.
        boolean allowMismatchedResults = false;
        if (plannedStmtBatch.isReadOnly()) {
            for (AdHocPlannedStatement stmt : plannedStmtBatch.plannedStatements) {
                if (stmt.core.isNonDeterministic) {
                    allowMismatchedResults = true;
                    break;
                }
            }
        }
        // initiate the transaction
        createTransaction(plannedStmtBatch.connectionId, plannedStmtBatch.hostname,
                plannedStmtBatch.adminConnection, task,
                plannedStmtBatch.isReadOnly(), isSinglePartition, false,
                partitions, partitions.length, plannedStmtBatch.clientData,
                serializedSize, EstTime.currentTimeMillis(), allowMismatchedResults);
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

                        // assume all stmts have the same catalog version
                        if ((plannedStmtBatch.getPlannedStatementCount() > 0) &&
                            (plannedStmtBatch.getPlannedStatement(0).core.catalogVersion != m_catalogContext.get().catalogVersion)) {

                            /* The adhoc planner learns of catalog updates after the EE and the
                               rest of the system. If the adhoc sql was planned against an
                               obsolete catalog, re-plan. */
                            LocalObjectMessage work = new LocalObjectMessage(
                                    new AdHocPlannerWork(m_siteId,
                                            false,
                                            plannedStmtBatch.clientHandle,
                                            plannedStmtBatch.connectionId,
                                            plannedStmtBatch.hostname,
                                            plannedStmtBatch.adminConnection,
                                            plannedStmtBatch.clientData,
                                            plannedStmtBatch.sqlBatchText,
                                            plannedStmtBatch.getSQLStatements(),
                                            plannedStmtBatch.partitionParam,
                                            null,
                                            false,
                                            true,
                                            plannedStmtBatch.type,
                                            plannedStmtBatch.originalTxnId,
                                            plannedStmtBatch.originalTs,
                                            m_adhocCompletionHandler));

                            m_mailbox.send(m_plannerSiteId, work);
                        }
                        else if( plannedStmtBatch.isExplainWork() ) {
                            processExplainPlannedStmtBatch( plannedStmtBatch );
                        }
                        else {
                            createAdHocTransaction(plannedStmtBatch);
                        }
                    }
                    else if (result instanceof CatalogChangeResult) {
                        final CatalogChangeResult changeResult = (CatalogChangeResult) result;
                        // create the execution site task
                        StoredProcedureInvocation task = new StoredProcedureInvocation();
                        task.procName = "@UpdateApplicationCatalog";
                        task.setParams(changeResult.encodedDiffCommands, changeResult.catalogBytes,
                                       changeResult.expectedCatalogVersion, changeResult.deploymentString,
                                       changeResult.deploymentCRC);
                        task.clientHandle = changeResult.clientHandle;

                        /*
                         * Round trip the invocation to initialize it for command logging
                         */
                        FastSerializer fs = new FastSerializer();
                        try {
                            fs.writeObject(task);
                            ByteBuffer source = fs.getBuffer();
                            ByteBuffer copy = ByteBuffer.allocate(source.remaining());
                            copy.put(source);
                            copy.flip();
                            FastDeserializer fds = new FastDeserializer(copy);
                            task = new StoredProcedureInvocation();
                            task.readExternal(fds);
                        } catch (Exception e) {
                            hostLog.fatal(e);
                            VoltDB.crashLocalVoltDB(e.getMessage(), true, e);
                        }

                        // initiate the transaction. These hard-coded values from catalog
                        // procedure are horrible, horrible, horrible.
                        createTransaction(changeResult.connectionId, changeResult.hostname,
                                changeResult.adminConnection,
                                task, false, true, true, m_allPartitions,
                                m_allPartitions.length, changeResult.clientData, task.getSerializedSize(),
                                EstTime.currentTimeMillis(), false);
                    }
                    else {
                        throw new RuntimeException(
                                "Should not be able to get here (ClientInterface.checkForFinishedCompilerWork())");
                    }
                }
                else {
                    ClientResponseImpl errorResponse =
                        new ClientResponseImpl(
                                ClientResponseImpl.UNEXPECTED_FAILURE,
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
                    StringWriter sw = new StringWriter();
                    PrintWriter pw = new PrintWriter(sw);
                    e.printStackTrace(pw);
                    pw.flush();
                    ClientResponseImpl errorResponse =
                            new ClientResponseImpl(
                                    ClientResponseImpl.UNEXPECTED_FAILURE,
                                    new VoltTable[0], result.errorMsg,
                                    result.clientHandle);
                    ByteBuffer buf = ByteBuffer.allocate(errorResponse.getSerializedSize() + 4);
                    buf.putInt(buf.capacity() - 4);
                    errorResponse.flattenToBuffer(buf);
                    buf.flip();
                    c.writeStream().enqueue(buf);
                }
            }
        }, MoreExecutors.sameThreadExecutor());

        //Return the future task for test code
        return ft;
    }

    /**
     * Tick counter used to perform dead client detection every N ticks
     */
    private long m_tickCounter = 0;

    public final void processPeriodicWork() {
        long time;
        if (m_isIV2Enabled) {
            time = System.currentTimeMillis();
        }
        else {
            time = m_initiator.tick();
        }
        m_tickCounter++;
        if (m_tickCounter % 20 == 0) {
            checkForDeadConnections(time);
        }

        // check for catalog updates
        if (m_shouldUpdateCatalog.compareAndSet(true, false)) {
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

        return;
    }

    /**
     * Check for dead connections by providing each connection with the current
     * time so it can calculate the delta between now and the time the oldest message was
     * queued for sending.
     * @param now Current time in milliseconds
     */
    private final void checkForDeadConnections(final long now) {
        final ArrayList<Connection> connectionsToRemove = new ArrayList<Connection>();
        for (final Connection c : m_connections) {
            final int delta = c.writeStream().calculatePendingWriteDelta(now);
            if (delta > 4000) {
                connectionsToRemove.add(c);
            }
        }

        for (final Connection c : connectionsToRemove) {
            networkLog.warn("Closing connection to " + c + " at " + new java.util.Date() + " because it refuses to read responses");
            c.unregister();
        }
    }

    // BUG: this needs some more serious thinking
    // probably should be able to schedule a shutdown event
    // to the dispatcher..  Or write a "stop reading and flush
    // all your read buffers" events .. or something ..
    protected void shutdown() throws InterruptedException {
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
        if (m_iv2Masters != null) {
            m_iv2Masters.shutdown();
        }
        if (m_localReplicasBuilder != null) {
            m_localReplicasBuilder.join(10000);
            hostLog.error("Local replica map builder took more than ten seconds, probably hung");
            m_localReplicasBuilder.join();
        }
    }

    private volatile Thread m_localReplicasBuilder = null;
    public void startAcceptingConnections() throws IOException {
        if (m_isIV2Enabled) {
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
                    for (int partition : m_allPartitions) {
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
        }

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

    /**
     * Identify the partition for an execution site task.
     * @return The partition best set up to execute the procedure.
     * @throws Exception
     */
    int getPartitionForProcedure(int partitionIndex, int partitionType,
            StoredProcedureInvocation task)
    throws Exception
    {
        Object invocationParameter = task.getParameterAtIndex(partitionIndex);
        final VoltType partitionParamType = VoltType.get((byte)partitionType);

        // Special case: if the user supplied a string for a number column,
        // try to do the conversion. This makes it substantially easier to
        // load CSV data or other untyped inputs that match DDL without
        // requiring the loader to know precise the schema.
        if ((invocationParameter != null) &&
            (invocationParameter.getClass() == String.class) &&
            (partitionParamType.isNumber()))
        {
            invocationParameter = ParameterConverter.stringToLong(
                    invocationParameter,
                    partitionParamType.classFromType());
        }

        return TheHashinator.hashToPartition(invocationParameter);
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
                ParameterSet paramSet = new ParameterSet();
                paramSet.setParameters(params);
                return paramSet;
            }
        });
        spi.clientHandle = clientData;
        // initiate the transaction
        boolean allowMismatchedResults = catProc.getReadonly() && isProcedureNonDeterministic(catProc);
        createTransaction(m_snapshotDaemonAdapter.connectionId(),
                "SnapshotDaemon",
                true, // treat the snapshot daemon like it's on an admin port
                spi, catProc.getReadonly(),
                catProc.getSinglepartition(), catProc.getEverysite(),
                m_allPartitions, m_allPartitions.length,
                m_snapshotDaemonAdapter,
                0, EstTime.currentTimeMillis(),
                allowMismatchedResults);
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
        public String getHostnameOrIP() {
            return "";
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
        public void enqueue(final org.voltcore.utils.DeferredSerialization ds)
        {

            m_snapshotDaemon.processClientResponse(new Callable<ClientResponseImpl>() {
                @Override
                public ClientResponseImpl call() throws Exception {
                    ClientResponseImpl resp = new ClientResponseImpl();
                    ByteBuffer b = ds.serialize()[0];
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
            throw new UnsupportedOperationException();
        }
    }

    public Map<Long, Pair<String, long[]>> getLiveClientStats()
    {
        final Map<Long, Pair<String, long[]>> client_stats =
            new HashMap<Long, Pair<String, long[]>>();

        if (m_isIV2Enabled) {
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
        }
        else {
            Map<Long, long[]> inflight_txn_stats = m_initiator.getOutstandingTxnStats();

            // put all the live connections in the stats map, then fill in admin and
            // outstanding txn info from the inflight stats
            for (Connection c : m_connections) {
                if (!client_stats.containsKey(c.connectionId())) {
                    client_stats.put(
                            c.connectionId(),
                            new Pair<String, long[]>(c.getHostnameOrIP(),
                                new long[]{0,
                                    c.readStream().dataAvailable(),
                                    c.writeStream().getOutstandingMessageCount(),
                                    0})
                            );
                }
            }

            for (Entry<Long, long[]> stat : inflight_txn_stats.entrySet()) {
                if (client_stats.containsKey(stat.getKey())) {
                    client_stats.get(stat.getKey()).getSecond()[0] = stat.getValue()[0];
                    client_stats.get(stat.getKey()).getSecond()[3] = stat.getValue()[1];
                }
            }
        }
        return client_stats;
    }

    public SnapshotDaemon getSnapshotDaemon() {
        return m_snapshotDaemon;
    }

    public void sendSentinel(long txnId, int partitionId, boolean forReplay) {
        assert(m_isIV2Enabled);
        final long initiatorHSId = m_iv2Masters.get(partitionId);

        //The only field that is relevant is txnid, and forReplay.
        MultiPartitionParticipantMessage mppm =
                new MultiPartitionParticipantMessage(
                        initiatorHSId,
                        m_cartographer.getHSIdForMultiPartitionInitiator(),
                        txnId,
                        false,  // isReadOnly
                        true);  // isForReplay
        m_mailbox.send(initiatorHSId, mppm);
    }

    /**
     * Sends an end of log message to the master of that partition. This should
     * only be called at the end of replay.
     *
     * @param partitionId
     */
    public void sendEOLMessage(int partitionId) {
        assert(m_isIV2Enabled);
        final long initiatorHSId = m_iv2Masters.get(partitionId);
        Iv2EndOfLogMessage message = new Iv2EndOfLogMessage();
        m_mailbox.send(initiatorHSId, message);
    }

    public List<Iterator<Map.Entry<String, InvocationInfo>>> getIV2InitiatorStats() {
        ArrayList<Iterator<Map.Entry<String, InvocationInfo>>> statsIterators =
                new ArrayList<Iterator<Map.Entry<String, InvocationInfo>>>();
        for (AdmissionControlGroup acg : m_allACGs) {
            statsIterators.add(acg.getInitiationStatsIterator());
        }
        return statsIterators;
    }
}
