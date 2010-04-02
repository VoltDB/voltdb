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

package org.voltdb.messaging.impl;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import org.voltdb.VoltDB;
import org.voltdb.fault.NodeFailureFault;
import org.voltdb.messaging.FastSerializer;
import org.voltdb.messaging.VoltMessage;
import org.voltdb.network.Connection;
import org.voltdb.network.QueueMonitor;
import org.voltdb.network.VoltProtocolHandler;
import org.voltdb.utils.DBBPool;
import org.voltdb.utils.DeferredSerialization;
import org.voltdb.utils.DBBPool.BBContainer;

public class ForeignHost {
    private final Connection m_connection;
    private final FHInputHandler m_handler;
    private final HostMessenger m_hostMessenger;
    private final InetAddress m_ipAddress;
    private final int m_tcpPort;
    private final int m_hostId;

    @SuppressWarnings("unused")
    private String m_remoteHostname;
    private boolean m_closing;

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
            if (!m_closing)
            {
                VoltDB.instance().getFaultDistributor().
                reportFault(new NodeFailureFault(m_hostId));
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
    }

    void close()
    {
        m_closing = true;
        m_hostMessenger.getNetwork().unregisterChannel(m_connection);
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
    }

    /**
     * Send the message to the foreign host that this host is ready.
     * This should be the first message sent by this system.
     * It is differentiated from other messages because it is sent
     * to mailbox -1.
     */
    void sendReadyMessage() {
        InetAddress addr = null;
        try {
            addr = InetAddress.getLocalHost();
        } catch (UnknownHostException e) {
            e.printStackTrace();
            VoltDB.crashVoltDB();
        }
        String hostname = addr.getHostName();
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
