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

package org.voltcore.messaging;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltcore.network.PicoNetwork;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.EstTime;
import org.voltcore.utils.RateLimitedLogger;
import org.voltdb.VoltDB;

import com.google_voltpatches.common.base.Throwables;
import com.google_voltpatches.common.collect.ImmutableList;
import com.google_voltpatches.common.collect.ImmutableMap;
import com.google_voltpatches.common.primitives.Longs;

public class ForeignHost {
    private static final VoltLogger hostLog = new VoltLogger("HOST");
    private static RateLimitedLogger rateLimitedLogger;
    private static long m_logRate;

    private final HostMessenger m_hostMessenger;
    private final Integer m_hostId;
    private final String m_hostDisplayName;
    final InetSocketAddress m_listeningAddress;

    boolean m_isUp;

    // Each foreign host may have one or many sub-connections, depends if partition
    // group is enabled (k-factor > 0 and hostCount > 3). Each sub-connection has
    // a pico-network thread serves for intra-cluster traffic.
    private ImmutableList<Subconnection> m_connections = ImmutableList.of();
    // reference to the first object of the connection list
    private final Subconnection m_firstConn;

    // Remote site id to sub-connection mapping
    volatile ImmutableMap<Long, Subconnection> m_connByHSIds = ImmutableMap.of();
    // Some site ids are resevered for special purposes, e.g. stats, snapshot, DR,
    // elastic expand/shrink etc., see comments in HostMessenger. Data traffic to those
    // special site ids is small related to normal transactional data. To avoid the disruption
    // of hsid-to-piconetwork binding of the normal data, use a separate map to evenly
    // distribute special site ids.
    volatile ImmutableMap<Long, Subconnection> m_connBySpecialHSIds = ImmutableMap.of();

    // A counter used to uniformly bind remote site ids to sub-connections (if any)
    private int m_nextConnection = 0;
    private int m_nextConnectionForSpecialHSId = 0;

    // Set the default here for TestMessaging, which currently has no VoltDB instance
    private long m_deadHostTimeout;
    private final AtomicLong m_lastMessageMillis = new AtomicLong(Long.MAX_VALUE);

    private final AtomicInteger m_deadReportsCount = new AtomicInteger(0);
    private final AtomicInteger m_connectionStoppingCount = new AtomicInteger(0);

    // Because the connections other than the first connection are created after cluster mesh network
    // has established but before the whole cluster has been initialized.
    // It's possible that some non-transactional iv2 messages to be sent through foreign host when
    // there is only one connection, So this flag is used to prevent binding all sites to the first
    // connection.
    private boolean m_hasMultiConnections;

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
        if (deadHostTimeout < 30 * 1000) {
            logRate = (int) (deadHostTimeout / 3);
        } else {
            logRate = 10 * 1000;
        }
        rateLimitedLogger = new RateLimitedLogger(logRate, hostLog, Level.WARN);
        m_logRate = logRate;
    }

    /**
     * Create a ForeignHost and install in VoltNetwork
     */
    ForeignHost(HostMessenger host,
                int hostId,
                String hostDisplayName,
                SocketChannel socket,
                int deadHostTimeout,
                InetSocketAddress listeningAddress,
                PicoNetwork network)
            throws IOException {
        m_hostMessenger = host;
        m_hostId = hostId;
        m_hostDisplayName = hostDisplayName;
        m_isUp = true;
        m_deadHostTimeout = deadHostTimeout;
        m_listeningAddress = listeningAddress;
        m_firstConn = new Subconnection(hostId, host, this, socket, network);
        addConnection(m_firstConn);

        setLogRate(deadHostTimeout);
    }

    public void enableRead(Set<Long> verbotenThreads) {
        for (Subconnection connection : m_connections) {
            connection.enableRead(verbotenThreads);
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
        Subconnection conn = new Subconnection(m_hostId, m_hostMessenger, this, socket, network);
        addConnection(conn);
        conn.enableRead(verbotenThreads);
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

    public void setHasMultiConnections() {
        m_hasMultiConnections = true;
    }

    public boolean getHasMultiConnections() {
        return m_hasMultiConnections;
    }

    @SuppressWarnings("deprecation")
    synchronized void close()
    {
        if (!m_isUp) {
            return;
        }
        m_isUp = false;
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
        m_isUp = true;
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
        close();
        super.finalize();
    }

    boolean isUp()
    {
        return m_isUp;
    }

    /** Send a message to the network. This public method is re-entrant. */
    void send(final long destinations[], final VoltMessage message) {
        if (destinations.length == 0) {
            return;
        }

        final HashMap<Subconnection, ArrayList<Long>> destinationsPerConn = new HashMap<>();
        if (m_hasMultiConnections) {
            for (long remoteHsId : destinations) {
                Subconnection c;
                // fast path
                // Negative site id is reserved for special purposes, deal them separately.
                if (remoteHsId < 0) {
                    c = m_connBySpecialHSIds.get(remoteHsId);
                } else {
                    c = m_connByHSIds.get(remoteHsId);
                }
                if (c == null) {
                    // slow path
                    c = bindConnection(remoteHsId);
                }
                ArrayList<Long> bundle = destinationsPerConn.get(c);
                if (bundle == null) {
                    bundle = new ArrayList<>();
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

        // NodeFailureFault no longer immediately trips FHInputHandler to
        // set m_isUp to false, so use both that and m_closing to
        // avoid repeat reports of a single node failure
        long current_time = EstTime.currentTimeMillis();
        long last_time = m_lastMessageMillis.get();
        long current_delta = current_time - last_time;
        if (m_isUp && current_delta > m_deadHostTimeout) {
            if (m_deadReportsCount.getAndIncrement() == 0) {
                hostLog.error("DEAD HOST DETECTED, hostname: " + hostname());
                hostLog.info("\tcurrent time: " + current_time);
                hostLog.info("\tlast message: " + last_time);
                hostLog.info("\tdelta (millis): " + current_delta);
                hostLog.info("\ttimeout value (millis): " + m_deadHostTimeout);
                VoltDB.dropStackTrace("Timed out foreign host " + hostname());
            }
            m_hostMessenger.reportForeignHostFailed(m_hostId);
        }

        //Try and give some warning when a connection is timing out.
        //Allows you to observe the liveness of the host receiving the heartbeats
        if (current_delta > m_logRate) {
            rateLimitedLogger.log(
                    "Have not received a message from host "
                        + hostnameAndIPAndPort() + " for " + (current_delta / 1000.0) + " seconds",
                        current_time);
        }

    }

    public void updateLastMessageTime(long lastMessageMillis) {
        if (lastMessageMillis > m_lastMessageMillis.get() || m_lastMessageMillis.get() == Long.MAX_VALUE) {
            m_lastMessageMillis.set(lastMessageMillis);
        }
    }

    // First report of connection hangup will kick-off fault resolution
    public void connectionStopping(Subconnection conn) {
        m_isUp = false;
        if (m_connectionStoppingCount.getAndIncrement() == 0) {
            // Log the remote host's action
            if (!m_hostMessenger.isShuttingDown()) {
                String msg = "Received remote hangup from foreign host " + conn.getHostnameOrIP();
                VoltDB.dropStackTrace(msg);
                CoreUtils.logWithEmphasis(hostLog, msg, Level.INFO);
            }
            m_hostMessenger.reportForeignHostFailed(m_hostId);
            m_hostMessenger.markPicoZombieHost(m_hostId);
        }
    }

    private Subconnection bindConnection(Long hsId) {
        synchronized (m_connectionLock) {
            // Negative site id is reserved for special purposes, deal them separately.
            Subconnection c;
            ImmutableMap.Builder<Long, Subconnection> b = ImmutableMap.builder();
            if (hsId < 0) {
                if ((c = m_connBySpecialHSIds.get(hsId)) == null) {
                    c = m_connections.get(++m_nextConnectionForSpecialHSId % m_connections.size());
                    m_connBySpecialHSIds = b.putAll(m_connBySpecialHSIds)
                            .put(hsId, c)
                            .build();
                }
            } else {
                if ((c = m_connByHSIds.get(hsId)) == null) {
                    c = m_connections.get(++m_nextConnection % m_connections.size());
                    m_connByHSIds = b.putAll(m_connByHSIds)
                            .put(hsId, c)
                            .build();
                }
            }
            return c;
        }
    }

    // Poison pill doesn't need remote hsid, remote host handles it immediately.
    public void sendPoisonPill(String err, int cause) {
        m_firstConn.sendPoisonPill(err, cause);
    }

    public FutureTask<Void> sendStopNodeNotice(int targetHostId) {
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

    public void updateDeadReportCount() {
        m_deadReportsCount.incrementAndGet();
    }

    /**
     * Test only method
     * used to immediately cut off reads from a foreign host
     * great way to trigger a heartbeat timout / simulate a network partition
     */
    void cutLink() {
        for (Subconnection c : m_connections) {
            c.cutLink();
        }
    }

    public String hostDisplayName() {
        return m_hostDisplayName;
    }
}
