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

package org.voltcore.messaging;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.google_voltpatches.common.base.Throwables;

import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltcore.network.Connection;
import org.voltcore.network.PicoNetwork;
import org.voltcore.network.QueueMonitor;
import org.voltcore.network.VoltProtocolHandler;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.DeferredSerialization;
import org.voltcore.utils.EstTime;
import org.voltcore.utils.RateLimitedLogger;
import org.voltdb.OperationMode;
import org.voltdb.VoltDB;

public class ForeignHost {
    private static final VoltLogger hostLog = new VoltLogger("HOST");
    private static final RateLimitedLogger rateLimitedLogger = new RateLimitedLogger(10 * 1000, hostLog, Level.WARN);

    final PicoNetwork m_network;
    final FHInputHandler m_handler;
    private final HostMessenger m_hostMessenger;
    private final Integer m_hostId;
    final InetSocketAddress m_listeningAddress;

    private boolean m_closing;
    boolean m_isUp;

    // hold onto the socket so we can kill it
    private final Socket m_socket;

    // Set the default here for TestMessaging, which currently has no VoltDB instance
    private long m_deadHostTimeout;
    private final AtomicLong m_lastMessageMillis = new AtomicLong(Long.MAX_VALUE);

    private final AtomicInteger m_deadReportsCount = new AtomicInteger(0);

    public static final int POISON_PILL = -1;

    public static final int CRASH_ALL = 0;
    public static final int CRASH_ME = 1;
    public static final int CRASH_SPECIFIED = 2;

    /** ForeignHost's implementation of InputHandler */
    public class FHInputHandler extends VoltProtocolHandler {

        @Override
        public int getMaxRead() {
            return Integer.MAX_VALUE;
        }

        @Override
        public void handleMessage(ByteBuffer message, Connection c) throws IOException {
            handleRead(message, c);
        }

        @Override
        public void stopping(Connection c)
        {
            m_isUp = false;
            if (!m_closing)
            {
                if (!m_hostMessenger.isShuttingDown()) {
                    VoltDB.dropStackTrace("Received remote hangup from foreign host " + hostnameAndIPAndPort());
                    hostLog.warn("Received remote hangup from foreign host " + hostnameAndIPAndPort());
                }
                m_hostMessenger.reportForeignHostFailed(m_hostId);
            }
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

    /** Create a ForeignHost and install in VoltNetwork */
    ForeignHost(HostMessenger host, int hostId, SocketChannel socket, int deadHostTimeout,
            InetSocketAddress listeningAddress, PicoNetwork network)
    throws IOException
    {
        m_hostMessenger = host;
        m_handler = new FHInputHandler();
        m_hostId = hostId;
        m_closing = false;
        m_isUp = true;
        m_socket = socket.socket();
        m_deadHostTimeout = deadHostTimeout;
        m_listeningAddress = listeningAddress;
        m_network = network;
    }

    public void enableRead(Set<Long> verbotenThreads) {
        m_network.start(m_handler, verbotenThreads);
    }

    synchronized void close()
    {
        m_isUp = false;
        if (m_closing) return;
        m_closing = true;
        try {
            m_network.shutdownAsync();
        } catch (InterruptedException e) {
            Throwables.propagate(e);
        }
    }

    /**
     * Used only for test code to kill this FH
     */
    void killSocket() {
        try {
            m_closing = true;
            m_socket.setKeepAlive(false);
            m_socket.setSoLinger(false, 0);
            Thread.sleep(25);
            m_socket.close();
            Thread.sleep(25);
            System.gc();
            Thread.sleep(25);
        }
        catch (Exception e) {
            // don't REALLY care if this fails
            e.printStackTrace();
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
        return m_isUp;
    }

    /** Send a message to the network. This public method is re-entrant. */
    void send(
            final long destinations[],
            final VoltMessage message)
    {
        if (destinations.length == 0) {
            return;
        }

        m_network.enqueue(
                new DeferredSerialization() {
                    @Override
                    public final void serialize(final ByteBuffer buf) throws IOException {
                        buf.putInt(buf.capacity() - 4);
                        buf.putLong(message.m_sourceHSId);
                        buf.putInt(destinations.length);
                        for (int ii = 0; ii < destinations.length; ii++) {
                            buf.putLong(destinations[ii]);
                        }
                        message.flattenToBuffer(buf);
                        buf.flip();
                    }

                    @Override
                    public final void cancel() {
                    /*
                     * Can this be removed?
                     */
                    }

                    @Override
                    public String toString() {
                        return message.getClass().getName();
                    }

                    @Override
                    public int getSerializedSize() {
                        final int len = 4            /* length prefix */
                                + 8            /* source hsid */
                                + 4            /* destinationCount */
                                + 8 * destinations.length  /* destination list */
                                + message.getSerializedSize();
                        return len;
                    }
                });

        long current_time = EstTime.currentTimeMillis();
        long current_delta = current_time - m_lastMessageMillis.get();
        /*
         * Try and give some warning when a connection is timing out.
         * Allows you to observe the liveness of the host receiving the heartbeats
         */
        if (current_delta > 10 * 1000) {
            rateLimitedLogger.log(
                    "Have not received a message from host "
                        + hostnameAndIPAndPort() + " for " + (current_delta / 1000.0) + " seconds",
                        current_time);
        }
        // NodeFailureFault no longer immediately trips FHInputHandler to
        // set m_isUp to false, so use both that and m_closing to
        // avoid repeat reports of a single node failure
        if ((!m_closing && m_isUp) &&
            (current_delta > m_deadHostTimeout))
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


    String hostnameAndIPAndPort() {
        return m_network.getHostnameAndIPAndPort();
    }

    String hostname() {
        return m_network.getHostnameOrIP();
    }

    /** Deliver a deserialized message from the network to a local mailbox */
    private void deliverMessage(long destinationHSId, VoltMessage message) {
        if (!m_hostMessenger.validateForeignHostId(m_hostId)) {
            hostLog.warn(String.format("Message (%s) sent to site id: %s @ (%s) at " +
                    m_hostMessenger.getHostId() + " from " + CoreUtils.hsIdToString(message.m_sourceHSId) +
                    " which is a known failed host. The message will be dropped\n",
                    message.getClass().getSimpleName(),
                    CoreUtils.hsIdToString(destinationHSId), m_socket.getRemoteSocketAddress().toString()));
            return;
        }

        Mailbox mailbox = m_hostMessenger.getMailbox(destinationHSId);
        /*
         * At this point we are OK with messages going to sites that don't exist
         * because we are saying that things can come and go
         */
        if (mailbox == null) {
            hostLog.info(String.format("Message (%s) sent to unknown site id: %s @ (%s) at " +
                    m_hostMessenger.getHostId() + " from " + CoreUtils.hsIdToString(message.m_sourceHSId) + "\n",
                    message.getClass().getSimpleName(),
                    CoreUtils.hsIdToString(destinationHSId), m_socket.getRemoteSocketAddress().toString()));
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

    /** Read data from the network. Runs in the context of Port when
     * data is available.
     * @throws IOException
     */
    private void handleRead(ByteBuffer in, Connection c) throws IOException {
        // port is locked by VoltNetwork when in valid use.
        // assert(m_port.m_lock.tryLock() == true);
        long recvDests[] = null;

        final long sourceHSId = in.getLong();
        final int destCount = in.getInt();
        if (destCount == POISON_PILL) {//This is a poison pill
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
            } else {
                //Should never come here.
                hostLog.error("Invalid Cause in poison pill: " + cause);
            }
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

        //m_lastMessageMillis = System.currentTimeMillis();
        m_lastMessageMillis.lazySet(EstTime.currentTimeMillis());

    }

    public void sendPoisonPill(String err, int cause) {
        byte errBytes[];
        try {
            errBytes = err.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            return;
        }
        ByteBuffer message = ByteBuffer.allocate(24 + errBytes.length);
        message.putInt(message.capacity() - 4);
        message.putLong(-1);
        message.putInt(POISON_PILL);
        message.putInt(errBytes.length);
        message.put(errBytes);
        message.putInt(cause);
        message.flip();
        m_network.enqueue(message);
    }

    public void updateDeadHostTimeout(int timeout) {
        m_deadHostTimeout = timeout;
    }
}
