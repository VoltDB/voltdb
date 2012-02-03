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

package org.voltcore.messaging;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.List;

import org.voltcore.logging.VoltLogger;
import org.voltcore.network.Connection;
import org.voltcore.network.QueueMonitor;
import org.voltcore.network.VoltProtocolHandler;
import org.voltcore.utils.DeferredSerialization;
import org.voltcore.utils.EstTime;
import org.voltcore.utils.MiscUtils;

public class ForeignHost {
    private static final VoltLogger hostLog = new VoltLogger("HOST");

    private Connection m_connection;
    final FHInputHandler m_handler;
    private final HostMessenger m_hostMessenger;
    final int m_hostId;
    final InetSocketAddress m_listeningAddress;

    private final String m_remoteHostname = "UNKNOWN_HOSTNAME";
    private boolean m_closing;
    boolean m_isUp;

    // hold onto the socket so we can kill it
    private final Socket m_socket;
    private final SocketChannel m_sc;

    // Set the default here for TestMessaging, which currently has no VoltDB instance
    private final long m_deadHostTimeout;
    private long m_lastMessageMillis;

    /** ForeignHost's implementation of InputHandler */
    public class FHInputHandler extends VoltProtocolHandler {

        @Override
        public int getMaxRead() {
            return Integer.MAX_VALUE;
        }

        @Override
        public int getExpectedOutgoingMessageSize() {
            return 2048;
        }

        @Override
        public void handleMessage(ByteBuffer message, Connection c) {
            handleRead(message, c);
        }

        @Override
        public void stopping(Connection c)
        {
            m_isUp = false;
            if (!m_closing)
            {
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
            InetSocketAddress listeningAddress)
    throws IOException
    {
        m_hostMessenger = host;
        m_handler = new FHInputHandler();
        m_hostId = hostId;
        m_closing = false;
        m_isUp = true;
        m_lastMessageMillis = Long.MAX_VALUE;
        m_sc = socket;
        m_socket = socket.socket();
        m_deadHostTimeout = deadHostTimeout;
        m_listeningAddress = listeningAddress;
        hostLog.info("Heartbeat timeout to host: " + m_socket.getRemoteSocketAddress() + " is " +
                         m_deadHostTimeout + " milliseconds");
    }

    public void register(HostMessenger host) throws IOException {
        m_connection = host.getNetwork().registerChannel( m_sc, m_handler);
    }

    synchronized void close()
    {
        m_isUp = false;
        if (m_closing) return;
        m_closing = true;
        if (m_connection != null)
            m_connection.unregister();
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
            final List<Long> destinations,
            final VoltMessage message)
    {
        if (destinations.isEmpty()) {
            return;
        }

        m_connection.writeStream().enqueue(
            new DeferredSerialization() {
                @Override
                public final ByteBuffer[] serialize() throws IOException{
                    int len = 4            /* length prefix */
                            + 8            /* source hsid */
                            + 4            /* destinationCount */
                            + 8 * destinations.size()  /* destination list */
                            + message.getSerializedSize();
                    ByteBuffer buf = ByteBuffer.allocate(len);
                    buf.putInt(len - 4);
                    buf.putLong(message.m_sourceHSId);
                    buf.putInt(destinations.size());
                    for (int ii = 0; ii < destinations.size(); ii++) {
                        buf.putLong(destinations.get(ii));
                    }
                    message.flattenToBuffer(buf);
                    return new ByteBuffer[] { buf };
                }

                @Override
                public final void cancel() {
                    /*
                     * Can this be removed?
                     */
                }
            });

        long current_time = EstTime.currentTimeMillis();
        long current_delta = current_time - m_lastMessageMillis;
        // NodeFailureFault no longer immediately trips FHInputHandler to
        // set m_isUp to false, so use both that and m_closing to
        // avoid repeat reports of a single node failure
        if ((!m_closing && m_isUp) &&
            (current_delta > m_deadHostTimeout))
        {
            hostLog.error("DEAD HOST DETECTED, hostname: " + m_remoteHostname);
            hostLog.info("\tcurrent time: " + current_time);
            hostLog.info("\tlast message: " + m_lastMessageMillis);
            hostLog.info("\tdelta (millis): " + current_delta);
            hostLog.info("\ttimeout value (millis): " + m_deadHostTimeout);
            m_hostMessenger.reportForeignHostFailed(m_hostId);
        }
//        else
//        {
//            if (current_delta > 0)
//            {
//                m_histo[(int)(current_delta / 100)]++;
//                m_deltas++;
//                if (m_deltas > 200000)
//                {
//                    System.out.println("Delta histo to host: " + m_hostId);
//                    for (int i = 0; i < 11; i++)
//                    {
//                        System.out.println("\t" + i + ": " + m_histo[i]);
//                    }
//                    m_deltas = 0;
//                }
//            }
//        }
    }


    String hostname() {
        return m_remoteHostname;
    }

    /** Deliver a deserialized message from the network to a local mailbox */
    private void deliverMessage(long destinationHSId, VoltMessage message) {
        Mailbox mailbox = m_hostMessenger.getMailbox(destinationHSId);
        if (mailbox == null) {
            System.err.printf("Message (%s) sent to unknown site id: %s @ (%s) at " +
                    m_hostMessenger.getHostId() + " from " + MiscUtils.hsIdToString(message.m_sourceHSId) + "\n",
                    message.getClass().getSimpleName(),
                    MiscUtils.hsIdToString(destinationHSId), m_socket.getRemoteSocketAddress().toString());
            /*
             * If it is for the wrong host, that definitely isn't cool
             */
            if (m_hostMessenger.getHostId() != (int)destinationHSId) {
                assert(false);
            }
            return;
        }
        // deliver the message to the mailbox
        mailbox.deliver(message);
    }

    /** Read data from the network. Runs in the context of Port when
     * data is available.
     */
    private void handleRead(ByteBuffer in, Connection c) {
        // port is locked by VoltNetwork when in valid use.
        // assert(m_port.m_lock.tryLock() == true);
        long recvDests[] = null;

        final long sourceHSId = in.getLong();
        final int destCount = in.getInt();
        recvDests = new long[destCount];
        for (int i = 0; i < destCount; i++) {
            recvDests[i] = in.getLong();
        }

        final VoltMessage message = VoltMessage.createMessageFromBuffer(in, sourceHSId);

        for (int i = 0; i < destCount; i++) {
            deliverMessage( recvDests[i], message);
        }

        //m_lastMessageMillis = System.currentTimeMillis();
        m_lastMessageMillis = EstTime.currentTimeMillis();

        // ENG-1608.  We sniff for FailureSiteUpdateMessages here so
        // that a node will participate in the failure resolution protocol
        // even if it hasn't directly witnessed a node fault.
        if (message instanceof FailureSiteUpdateMessage)
        {
            for (long failedHostId : ((FailureSiteUpdateMessage)message).m_failedHSIds) {
                m_hostMessenger.reportForeignHostFailed((int)failedHostId);
            }
        }
    }
}
