/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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

package org.voltdb.messaging;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import org.apache.log4j.Logger;
import org.voltdb.VoltDB;
import org.voltdb.client.ConnectionUtil;
import org.voltdb.fault.NodeFailureFault;
import org.voltdb.network.Connection;
import org.voltdb.network.QueueMonitor;
import org.voltdb.network.VoltProtocolHandler;
import org.voltdb.utils.DBBPool;
import org.voltdb.utils.DeferredSerialization;
import org.voltdb.utils.EstTime;
import org.voltdb.utils.VoltLoggerFactory;
import org.voltdb.utils.DBBPool.BBContainer;

public class ForeignHost {
    private static final Logger hostLog =
        Logger.getLogger("HOST", VoltLoggerFactory.instance());

    // The amount of time we allow between messages from a host
    // before deciding that it must be dead.  In millis.
    static final int DEAD_HOST_TIMEOUT_THRESHOLD = 10000;

    private final Connection m_connection;
    private final FHInputHandler m_handler;
    private final HostMessenger m_hostMessenger;
    private final InetAddress m_ipAddress;
    private final int m_tcpPort;
    private final int m_hostId;

    private String m_remoteHostname;
    private boolean m_closing;
    private boolean m_isUp;

    private long m_lastMessageMillis;

    /** ForeignHost's implementation of InputHandler */
    public class FHInputHandler extends VoltProtocolHandler {

        @Override
        public int getMaxRead() {
            return Integer.MAX_VALUE;
        }

        @Override
        public int getExpectedOutgoingMessageSize() {
            return FastSerializer.INITIAL_ALLOCATION;
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
                VoltDB.instance().getFaultDistributor().
                reportFault(new NodeFailureFault(m_hostId, m_remoteHostname));
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
    ForeignHost(HostMessenger host, int hostId, SocketChannel socket)
    throws IOException
    {
        m_hostMessenger = host;
        m_handler = new FHInputHandler();
        m_ipAddress = socket.socket().getInetAddress();
        m_tcpPort = socket.socket().getLocalPort();
        m_connection = host.getNetwork().registerChannel( socket, m_handler);
        m_hostId = hostId;
        m_closing = false;
        m_isUp = true;
        m_lastMessageMillis = Long.MAX_VALUE;
    }

    void close()
    {
        m_closing = true;
        m_connection.unregister();
    }

    boolean isUp()
    {
        return m_isUp;
    }

    /** Send a message to the network. This public method is re-entrant. */
    void send(final int mailboxId,
            final int destinations[],
            final int destinationCount,
            final DeferredSerialization message)
    {
        assert(destinationCount > 0);
        assert(destinationCount <= destinations.length);
        assert(destinationCount <= VoltMessage.MAX_DESTINATIONS_PER_HOST);

        if (destinationCount == 0) {
            return;
        }

        m_connection.writeStream().enqueue(
            new DeferredSerialization() {
                @Override
                public final BBContainer serialize(final DBBPool pool) throws IOException{
                    final BBContainer outContainer = message.serialize(pool);
                    ByteBuffer out = outContainer.b;
                    int len = out.limit() - VoltMessage.HEADER_SIZE;

                    int headerlen = 4                     /* mailboxId */
                                  + 4                     /* destinationCount */
                                  + 4 * destinationCount; /* destination list */

                    out.position(VoltMessage.HEADER_SIZE - headerlen - 4);
                    out.mark();
                    out.putInt(headerlen + len);
                    out.putInt(mailboxId);
                    out.putInt(destinationCount);
                    for (int i = 0; i < destinationCount; i++) {
                        out.putInt(destinations[i]);
                    }
                    out.reset();
                    return outContainer;
                }

                @Override
                public final void cancel() {
                    message.cancel();
                }
            });

        long current_time = EstTime.currentTimeMillis();
        long current_delta = current_time - m_lastMessageMillis;
        // NodeFailureFault no longer immediately trips FHInputHandler to
        // set m_isUp to false, so use both that and m_closing to
        // avoid repeat reports of a single node failure
        if ((!m_closing && m_isUp) &&
            (current_delta > DEAD_HOST_TIMEOUT_THRESHOLD))
        {
            hostLog.error("DEAD HOST DETECTED, hostname: " + m_remoteHostname);
            hostLog.info("\tcurrent time: " + current_time);
            hostLog.info("\tlast message: " + m_lastMessageMillis);
            hostLog.info("\tdelta: " + current_delta);
            close();
            VoltDB.instance().getFaultDistributor().
            reportFault(new NodeFailureFault(m_hostId, m_remoteHostname));
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

    /**
     * Send the message to the foreign host that this host is ready.
     * This should be the first message sent by this system.
     * It is differentiated from other messages because it is sent
     * to mailbox -1.
     */
    void sendReadyMessage() {
        String hostname = ConnectionUtil.getHostnameOrAddress();
        byte hostnameBytes[] = hostname.getBytes();
        ByteBuffer out = ByteBuffer.allocate(12 + hostnameBytes.length);
        // length prefix int
        out.putInt(8  + hostnameBytes.length);
        // sign that this is a ready message
        out.putInt(-1);
        // host id of the sender
        out.putInt(m_hostMessenger.getHostId());
        out.put(hostnameBytes);
        out.rewind();
        m_connection.writeStream().enqueue(out);
    }

    /** Deliver a deserialized message from the network to a local mailbox */
    private void deliverMessage(int siteId, int mailboxId, VoltMessage message) {
        // get the site and print an error if it can't be gotten
        MessengerSite site = m_hostMessenger.getSite(siteId);
        if (site == null) {
            // messaging system may do this in multi-way send.
            return;
        }

        // get the mailbox and print an error if it can't be gotten
        SiteMailbox mailbox = site.getMailbox(mailboxId);
        if (mailbox == null) {
            System.err.printf("Message sent to unknown mailbox id: %d:%d @ (%s:%d)\n",
                    siteId, mailboxId, m_ipAddress.toString(), m_tcpPort);
            assert(false);
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
        int recvDests[] = null;

        // read the header data for the message
        int mailboxId = in.getInt();

        // handle the ready message
        // this should be the first message recieved
        // it is sent to mailbox -1 and contains just the sender's host id
        if (mailboxId == -1) {
            int readyHost = in.getInt();
            byte hostnameBytes[] = new byte[in.remaining()];
            in.get(hostnameBytes);
            m_remoteHostname = new String(hostnameBytes);
            m_hostMessenger.hostIsReady(readyHost);

            return;
        }

        //m_lastMessageMillis = System.currentTimeMillis();
        m_lastMessageMillis = EstTime.currentTimeMillis();

        assert (mailboxId > -1);
        int destCount = in.getInt();
        assert (destCount > 0 && destCount < 1024);
        recvDests = new int[destCount];
        for (int i = 0; i < destCount; i++) {
            recvDests[i] = in.getInt();
        }

        int datalen = in.limit() - in.position();
        assert (in.limit() != datalen + VoltMessage.HEADER_SIZE);
        ByteBuffer raw = ByteBuffer.allocate(datalen + VoltMessage.HEADER_SIZE);
        raw.position(VoltMessage.HEADER_SIZE);
        raw.put(in);
        raw.position(VoltMessage.HEADER_SIZE);
        VoltMessage message = VoltMessage.createMessageFromBuffer(raw, true);
        // deliver to each destination mbox
        for (int i = 0; i < destCount; i++) {
            deliverMessage(recvDests[i], mailboxId, message);
        }
    }
}
