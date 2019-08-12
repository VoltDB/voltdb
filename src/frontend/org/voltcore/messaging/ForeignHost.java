/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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

package org.voltcore.messaging;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltcore.network.Connection;
import org.voltcore.network.PicoNetwork;
import org.voltcore.network.QueueMonitor;
import org.voltcore.network.VoltProtocolHandler;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.EstTime;
import org.voltcore.utils.RateLimitedLogger;
import org.voltdb.OperationMode;
import org.voltdb.VoltDB;

import com.google_voltpatches.common.base.Throwables;
import com.google_voltpatches.common.collect.ImmutableList;
import com.google_voltpatches.common.collect.ImmutableMap;
import com.google_voltpatches.common.primitives.Longs;

public class ForeignHost {
    private static final VoltLogger hostLog = new VoltLogger("HOST");
    private static RateLimitedLogger rateLimitedLogger;
    private static long m_logRate;

    final FHInputHandler m_handler;
    private final HostMessenger m_hostMessenger;
    private final Integer m_hostId;
    final InetSocketAddress m_listeningAddress;

    private boolean m_closing;
    AtomicBoolean m_isUp = new AtomicBoolean(true);

    // Each foreign host may have one or many sub-connections, depends if partition
    // group is enabled (k-factor > 0 and hostCount > 3). Each sub-connection has
    // a pico-network thread serves for intra-cluster traffic.
    private ImmutableList<Subconnection> m_connections = ImmutableList.of();
    // reference to the first object of the connection list
    private Subconnection m_firstConn;

    // hold onto the socket so we can kill it
    private final Socket m_socket;

    // Remote site id to sub-connection mapping
    volatile ImmutableMap<Long, Subconnection> m_connByHSIds = ImmutableMap.of();

    // A counter used to uniformly bind remote site ids to sub-connections (if any)
    private final AtomicInteger m_nextConnection = new AtomicInteger(0);

    // Set the default here for TestMessaging, which currently has no VoltDB instance
    private long m_deadHostTimeout;
    private final AtomicLong m_lastMessageMillis = new AtomicLong(Long.MAX_VALUE);

    private final AtomicInteger m_deadReportsCount = new AtomicInteger(0);

    // Because the connections other than the first connection are created after cluster mesh network
    // has established but before the whole cluster has been initialized.
    // It's possible that some non-transactional iv2 messages to be sent through foreign host when
    // there is only one connection, So this flag is used to prevent binding all sites to the first
    // connection.
    private boolean m_hasMultiConnections;


    // used to immediately cut off reads from a foreign host
    // great way to trigger a heartbeat timout / simulate a network partition
    private AtomicBoolean m_linkCutForTest = new AtomicBoolean(false);

    private final Object m_connectionLock = new Object();

    // STOPNODE_NOTICE is used to prevent split-brain detection caused by POISON_PILL
    public static final int POISON_PILL = -1;
    public static final int STOPNODE_NOTICE = -2;

    /*
     *  Poison pill types
     */
    public static final int CRASH_ALL = 0;
    public static final int CRASH_ME = 1;
    public static final int CRASH_SPECIFIED = 2;
    public static final int PRINT_STACKTRACE = 3;

    private void setLogRate(long deadHostTimeout) {
        int logRate;
        if (deadHostTimeout < 30 * 1000)
            logRate = (int) (deadHostTimeout / 3);
        else
            logRate = 10 * 1000;
        rateLimitedLogger = new RateLimitedLogger(logRate, hostLog, Level.WARN);
        m_logRate = logRate;
    }

    /** Create a ForeignHost and install in VoltNetwork */
    ForeignHost(HostMessenger host, int hostId, SocketChannel socket, int deadHostTimeout,
            InetSocketAddress listeningAddress, PicoNetwork network)
    throws IOException
    {
        m_hostMessenger = host;
        m_handler = new FHInputHandler();
        m_hostId = hostId;
        m_closing = false;
        m_deadHostTimeout = deadHostTimeout;
        m_listeningAddress = listeningAddress;
        m_socket = socket.socket();
        m_firstConn = new Subconnection(hostId, socket, network);
        addConnection(m_firstConn);

        setLogRate(deadHostTimeout);
    }

    public void enableRead(Set<Long> verbotenThreads) {
        for (Subconnection connection : m_connections) {
            connection.enableRead(m_handler, verbotenThreads);
        }
    }

    private void addConnection(Subconnection conn) {
        synchronized (m_connectionLock) {
            m_connections = ImmutableList.<Subconnection>builder()
                                .addAll(m_connections)
                                .add(conn)
                                .build();
        }
    }

    public void createAndEnableNewConnection(SocketChannel socket, PicoNetwork network, Set<Long> verbotenThreads) {
        Subconnection conn = new Subconnection(m_hostId, socket, network);
        addConnection(conn);
        conn.enableRead(m_handler, verbotenThreads);
    }

    public int connectionNumber() {
        return m_connections.size();
    }

    public ArrayList<PicoNetwork> getPicoNetworks() {
        ArrayList<PicoNetwork> networks = new ArrayList<>();
        for (Subconnection conn : m_connections) {
            networks.add(conn.getPicoNetwork());
        }
        return networks;
    }

    /** ForeignHost's implementation of InputHandler */
    public class FHInputHandler extends VoltProtocolHandler {

        @Override
        public int getMaxRead() {
            return Integer.MAX_VALUE;
        }

        @Override
        public void handleMessage(ByteBuffer message, Connection c) throws IOException {
            // if this link is "gone silent" for partition tests, just drop the message on the floor
            if (m_linkCutForTest.get()) {
                return;
            }

            handleRead(message, c);
        }

        @Override
        public void stopping(Connection c)
        {
            if (!m_closing && m_isUp.compareAndSet(true, false))
            {
                // Log the remote host's action
                if (!m_hostMessenger.isShuttingDown()) {
                    String msg = "Received remote hangup from foreign host " + hostnameAndIPAndPort();
                    VoltDB.dropStackTrace(msg);
                    CoreUtils.printAsciiArtLog(hostLog, msg, Level.INFO);
                }
                m_hostMessenger.reportForeignHostFailed(m_hostId);
            }
            m_hostMessenger.markPicoZombieHost(m_hostId);
        }

        @Override
        public Runnable offBackPressure() {
            return new Runnable() {
                @Override
                public void run() {}
            };
        }

        @Override
        public Runnable onBackPressure() {
            return new Runnable() {
                @Override
                public void run() {}
            };
        }

        @Override
        public QueueMonitor writestreamMonitor() {
            return null;
        }
    }

    public void setHasMultiConnections() {
        m_hasMultiConnections = true;
    }

    /**
     * Read data from the network. Runs in the context of PicoNetwork thread when
     * data is available.
     * @throws IOException
     */
    private void handleRead(ByteBuffer in, Connection c) throws IOException {
        // port is locked by VoltNetwork when in valid use.
        // assert(m_port.m_lock.tryLock() == true);
        long recvDests[] = null;

        final long sourceHSId = in.getLong();
        final int destCount = in.getInt();
        if (destCount == ForeignHost.POISON_PILL) {//This is a poison pill
            //Ignore poison pill during shutdown, in tests we receive crash messages from
            //leader appointer during shutdown
            if (VoltDB.instance().getMode() == OperationMode.SHUTTINGDOWN) {
                return;
            }
            byte messageBytes[] = new byte[in.getInt()];
            in.get(messageBytes);
            String message = new String(messageBytes, "UTF-8");
            message = String.format("Fatal error from id,hostname(%d,%s): %s",
                    m_hostId, hostnameAndIPAndPort(), message);
            //if poison pill with particular cause handle it.
            int cause = in.getInt();
            if (cause == ForeignHost.CRASH_ME) {
                int hid = VoltDB.instance().getHostMessenger().getHostId();
                hostLog.debug("Poison Pill with target me was sent.: " + hid);
                //Killing myself.
                VoltDB.instance().halt();
            } else if (cause == ForeignHost.CRASH_ALL || cause == ForeignHost.CRASH_SPECIFIED) {
                org.voltdb.VoltDB.crashLocalVoltDB(message, false, null);
            } else if (cause == ForeignHost.PRINT_STACKTRACE) {
                //collect thread dumps
                String dumpDir = new File(VoltDB.instance().getVoltDBRootPath(), "thread_dumps").getAbsolutePath();
                String fileName =  m_hostMessenger.getHostname() + "_host-" + m_hostId + "_" + System.currentTimeMillis()+".jstack";
                VoltDB.dumpThreadTraceToFile(dumpDir, fileName );
            } else {
                //Should never come here.
                hostLog.error("Invalid Cause in poison pill: " + cause);
            }
            return;
        } else if (destCount == ForeignHost.STOPNODE_NOTICE) {
            int targetHostId = in.getInt();
            hostLog.info("Receive StopNode notice for host " + targetHostId);
            m_hostMessenger.addStopNodeNotice(targetHostId);
            return;
        }

        recvDests = new long[destCount];
        for (int i = 0; i < destCount; i++) {
            recvDests[i] = in.getLong();
        }

        final VoltMessage message =
            m_hostMessenger.getMessageFactory().createMessageFromBuffer(in, sourceHSId);

        // ENG-1608.  We sniff for SiteFailureMessage here so
        // that a node will participate in the failure resolution protocol
        // even if it hasn't directly witnessed a node fault.
        if (   message instanceof SiteFailureMessage
                && !(message instanceof SiteFailureForwardMessage))
        {
            SiteFailureMessage sfm = (SiteFailureMessage)message;
            for (FaultMessage fm: sfm.asFaultMessages()) {
                m_hostMessenger.relayForeignHostFailed(fm);
            }
        }

        for (int i = 0; i < destCount; i++) {
            deliverMessage( recvDests[i], message);
        }

        updateLastMessageTime(EstTime.currentTimeMillis());
    }

    /** Deliver a deserialized message from the network to a local mailbox */
    private void deliverMessage(long destinationHSId, VoltMessage message) {
        if (!m_hostMessenger.validateForeignHostId(m_hostId)) {
            hostLog.warn(String.format("Message (%s) sent to site id: %s @ (%s) at %d from %s "
                    + "which is a known failed host. The message will be dropped\n",
                    message.getClass().getSimpleName(),
                    CoreUtils.hsIdToString(destinationHSId),
                    m_socket.getRemoteSocketAddress().toString(),
                    m_hostMessenger.getHostId(),
                    CoreUtils.hsIdToString(message.m_sourceHSId)));
            return;
        }

        Mailbox mailbox = m_hostMessenger.getMailbox(destinationHSId);
        /*
         * At this point we are OK with messages going to sites that don't exist
         * because we are saying that things can come and go
         */
        if (mailbox == null) {
            hostLog.info(String.format("Message (%s) sent to unknown site id: %s @ (%s) at %d from %s \n",
                    message.getClass().getSimpleName(),
                    CoreUtils.hsIdToString(destinationHSId),
                    m_socket.getRemoteSocketAddress().toString(),
                    m_hostMessenger.getHostId(),
                    CoreUtils.hsIdToString(message.m_sourceHSId)));
            /*
             * If it is for the wrong host, that definitely isn't cool
             */
            if (m_hostMessenger.getHostId() != (int)destinationHSId) {
                VoltDB.crashLocalVoltDB("Received a message at wrong host", false, null);
            }
            return;
        }
        // deliver the message to the mailbox
        mailbox.deliver(message);
    }

    synchronized void close()
    {
        m_isUp.compareAndSet(true, false);
        if (m_closing) return;
        m_closing = true;
        try {
            for (Subconnection c : m_connections) {
                c.close();
            }
            m_connections = null;
        } catch (InterruptedException e) {
            Throwables.propagate(e);
        }
    }

    /**
     * Used only for test code to kill this FH
     */
    void killSocket() {
        m_closing = true;
        for (Subconnection c : m_connections) {
            c.killSocket();
        }
    }

    /*
     * Huh!? The constructor registers the ForeignHost with VoltNetwork so finalizer
     * will never get called unless the ForeignHost is unregistered with the VN.
     */
    @Override
    protected void finalize() throws Throwable
    {
        if (m_closing) return;
        close();
        super.finalize();
    }

    boolean isUp()
    {
        return m_isUp.get();
    }

    /** Send a message to the network. This public method is re-entrant. */
    void send(final long destinations[], final VoltMessage message) {
        if (!m_isUp.get()) {
            hostLog.warn("Failed to send VoltMessage because connection to host " +
                    CoreUtils.getHostIdFromHSId(destinations[0])+ " is closed");
            return;
        }
        if (destinations.length == 0) {
            return;
        }

        // if this link is "gone silent" for partition tests, just drop the message on the floor
        if (m_linkCutForTest.get()) {
            return;
        }

        final HashMap<Subconnection, ArrayList<Long>> destinationsPerConn =
                new HashMap<Subconnection, ArrayList<Long>>();
        if (m_hasMultiConnections) {
            for (long remoteHsId : destinations) {
                // fast path
                Subconnection c = m_connByHSIds.get(remoteHsId);
                if (c == null) {
                    // slow path, invoked when this host sends the first message for the destination
                    c = m_connections.get(m_nextConnection.getAndIncrement() % m_connections.size());
                    bindConnection(remoteHsId, c);
                }
                ArrayList<Long> bundle = destinationsPerConn.get(c);
                if (bundle == null) {
                    bundle = new ArrayList<Long>();
                    destinationsPerConn.put(c, bundle);
                }
                bundle.add(remoteHsId);
            }
            for (Entry<Subconnection, ArrayList<Long>> e : destinationsPerConn.entrySet()) {
                e.getKey().send(Longs.toArray(e.getValue()), message);
            }
        } else {
            m_firstConn.send(destinations, message);
        }

        detectDeadHost();
    }

    private void detectDeadHost() {
        long current_time = EstTime.currentTimeMillis();
        long current_delta = current_time - m_lastMessageMillis.get();
        /*
         * Try and give some warning when a connection is timing out.
         * Allows you to observe the liveness of the host receiving the heartbeats
         */
        if (current_delta > m_logRate) {
            rateLimitedLogger.log(
                    "Have not received a message from host "
                        + hostnameAndIPAndPort() + " for " + (current_delta / 1000.0) + " seconds",
                        current_time);
        }
        // NodeFailureFault no longer immediately trips FHInputHandler to
        // set m_isUp to false, so use both that and m_closing to
        // avoid repeat reports of a single node failure
        if ((!m_closing && m_isUp.get()) &&
                current_delta > m_deadHostTimeout)
        {
            if (m_deadReportsCount.getAndIncrement() == 0) {
                hostLog.error("DEAD HOST DETECTED, hostname: " + hostnameAndIPAndPort());
                hostLog.info("\tcurrent time: " + current_time);
                hostLog.info("\tlast message: " + m_lastMessageMillis);
                hostLog.info("\tdelta (millis): " + current_delta);
                hostLog.info("\ttimeout value (millis): " + m_deadHostTimeout);
                VoltDB.dropStackTrace("Timed out foreign host " + hostnameAndIPAndPort() + " with delta " + current_delta);
            }
            m_hostMessenger.reportForeignHostFailed(m_hostId);
        }
    }

    // LazySet doesn't guarantee atomic semantic, some threads may hold an older value
    // of the timestamp for a while, eventually the value will converge. Given that
    // heartbeat meassage is sent in every 25ms, the inconsistency window is acceptable.
    private void updateLastMessageTime(long lastMessageMillis) {
        m_lastMessageMillis.lazySet(lastMessageMillis);
    }

    private void bindConnection(Long hsId, Subconnection conn) {
        ImmutableMap.Builder<Long, Subconnection> b = ImmutableMap.builder();
        m_connByHSIds = b.putAll(m_connByHSIds)
                         .put(hsId, conn)
                         .build();
    }

    // Poison pill doesn't need remote hsid, remote host handles it immediately.
    public void sendPoisonPill(String err, int cause) {
        // if this link is "gone silent" for partition tests, just drop the message on the floor
        if (m_linkCutForTest.get()) {
            return;
        }
        m_firstConn.sendPoisonPill(err, cause);
    }

    public FutureTask<Void> sendStopNodeNotice(int targetHostId) {
        // if this link is "gone silent" for partition tests, just drop the message on the floor
        if (m_linkCutForTest.get()) {
            return null;
        }
        return m_firstConn.sendStopNodeNotice(targetHostId);
    }

    String hostnameAndIPAndPort() {
        return m_firstConn.getHostnameAndIPAndPort();
    }

    String hostname() {
        return m_firstConn.getHostnameOrIP();
    }

    public void updateDeadHostTimeout(int timeout) {
        m_deadHostTimeout = timeout;
        setLogRate(timeout);
    }

    /**
     * Test only method
     * used to immediately cut off reads from a foreign host
     * great way to trigger a heartbeat timout / simulate a network partition
     */
    void cutLink() {
        m_linkCutForTest.set(true);
    }
}
