/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Set;

import org.voltdb.VoltDB;
import org.voltdb.client.ConnectionUtil;
import org.voltdb.fault.FaultHandler;
import org.voltdb.fault.NodeFailureFault;
import org.voltdb.fault.VoltFault;
import org.voltdb.logging.VoltLogger;
import org.voltdb.network.Connection;
import org.voltdb.network.QueueMonitor;
import org.voltdb.network.VoltProtocolHandler;
import org.voltdb.utils.DBBPool;
import org.voltdb.utils.DBBPool.BBContainer;
import org.voltdb.utils.DeferredSerialization;
import org.voltdb.utils.EstTime;

public class ForeignHost {
    private static final VoltLogger hostLog = new VoltLogger("HOST");

    // special magic mailbox ids that get handled specially (and magically)
    public static final int READY_SIGNAL = -1;
    public static final int CATALOG_SIGNAL = -2;
    public static final int POISON_SIGNAL = -3;

    private final Connection m_connection;
    final FHInputHandler m_handler;
    private final HostMessenger m_hostMessenger;
    private final InetAddress m_ipAddress;
    private final int m_tcpPort;
    private final int m_hostId;

    private String m_remoteHostname = "UNKNOWN_HOSTNAME";
    private boolean m_closing;
    boolean m_isUp;

    // hold onto the socket so we can kill it
    private final Socket m_socket;

    // Set the default here for TestMessaging, which currently has no VoltDB instance
    private final long m_deadHostTimeout;
    private long m_lastMessageMillis;

    private class FHFaultHandler implements FaultHandler
    {
        @Override
        public void faultOccured(Set<VoltFault> faults)
        {
            for (VoltFault fault : faults) {
                if (fault instanceof NodeFailureFault)
                {
                    NodeFailureFault node_fault = (NodeFailureFault)fault;
                    if (node_fault.getHostId() == m_hostId) {
                        close();
                    }
                }
                VoltDB.instance().getFaultDistributor().reportFaultHandled(this, fault);
            }
        }

        @Override
        public void faultCleared(Set<VoltFault> faults) {
            // TODO Auto-generated method stub

        }
    }

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
                reportFault(new NodeFailureFault(
                        m_hostId,
                        VoltDB.instance().getCatalogContext().siteTracker.getNonExecSitesForHost(m_hostId),
                        m_remoteHostname));
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
        m_socket = socket.socket();
        m_deadHostTimeout = VoltDB.instance().getConfig().m_deadHostTimeoutMS;
        //TestMessaging doesn't have the real deal
        if (VoltDB.instance() != null) {
            if (VoltDB.instance().getFaultDistributor() != null) {
                VoltDB.instance().getFaultDistributor().
                    registerFaultHandler(
                            NodeFailureFault.NODE_FAILURE_FOREIGN_HOST,
                            new FHFaultHandler(),
                            org.voltdb.fault.VoltFault.FaultType.NODE_FAILURE);
            }
            hostLog.info("Heartbeat timeout to host: " + m_ipAddress + " is " +
                         m_deadHostTimeout + " milliseconds");
        }
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
            (current_delta > m_deadHostTimeout))
        {
            hostLog.error("DEAD HOST DETECTED, hostname: " + m_remoteHostname);
            hostLog.info("\tcurrent time: " + current_time);
            hostLog.info("\tlast message: " + m_lastMessageMillis);
            hostLog.info("\tdelta (millis): " + current_delta);
            hostLog.info("\ttimeout value (millis): " + m_deadHostTimeout);
            VoltDB.instance().getFaultDistributor().
            reportFault(new NodeFailureFault(
                    m_hostId,
                    VoltDB.instance().getCatalogContext().siteTracker.getNonExecSitesForHost(m_hostId),
                    m_remoteHostname));
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
     * This should be the first or second message sent by this system.
     * It may be preceded by a catalog send.
     * It is differentiated from other messages because it is sent
     * to mailbox -1 (READY_SIGNAL)
     */
    void sendReadyMessage() {
        String hostname = ConnectionUtil.getHostnameOrAddress();
        byte hostnameBytes[] = hostname.getBytes();
        ByteBuffer out = ByteBuffer.allocate(12 + hostnameBytes.length);
        // length prefix int
        out.putInt(8  + hostnameBytes.length);
        // sign that this is a ready message
        out.putInt(READY_SIGNAL);
        // host id of the sender
        out.putInt(m_hostMessenger.getHostId());
        out.put(hostnameBytes);
        out.rewind();
        m_connection.writeStream().enqueue(out);
    }

    /**
     * Send raw bytes to a mailbox, usually a catalog handler
     * mailbox or a poison pill handler mailbox.
     */
    void sendBytesToMailbox(int mailboxId, byte[] bytes) {
        ByteBuffer out = ByteBuffer.allocate(8 + bytes.length);
        // length prefix int
        out.putInt(bytes.length + 4);

        out.putInt(mailboxId);
        // byte data itself
        out.put(bytes);
        out.rewind();
        m_connection.writeStream().enqueue(out);
    }

    String hostname() {
        return m_remoteHostname;
    }

    String inetAddrString() {
        return m_ipAddress.toString();
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
        Mailbox mailbox = site.getMailbox(mailboxId);
        if (mailbox == null) {
            System.err.printf("Message (%s) sent to unknown mailbox id: %d:%d @ (%s:%d)\n",
                    message.getClass().getSimpleName(),
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
        // this should be the first message received
        // it is sent to mailbox -1 and contains just the sender's host id
        if (mailboxId == READY_SIGNAL) {
            int readyHost = in.getInt();
            byte hostnameBytes[] = new byte[in.remaining()];
            in.get(hostnameBytes);
            m_remoteHostname = new String(hostnameBytes);
            m_hostMessenger.hostIsReady(readyHost);
            return;
        }

        // handle a node sending us a catalog
        if (mailboxId == CATALOG_SIGNAL) {
            byte catalogBytes[] = new byte[in.remaining()];
            in.get(catalogBytes);
            VoltDB.instance().writeNetworkCatalogToTmp(catalogBytes);
            return;
        }

        // handle a request to crash the node
        if (mailboxId == POISON_SIGNAL) {
            byte messageBytes[] = new byte[in.remaining()];
            in.get(messageBytes);
            try {
                String msg = new String(messageBytes, "UTF-8");
                msg = String.format("Fatal error from id,hostname(%d,%s): %s",
                        m_hostId, hostname(), msg);
                VoltDB.crashLocalVoltDB(msg, false, null);
            } catch (UnsupportedEncodingException e) {
                VoltDB.crashLocalVoltDB("Should never get here", false, e);
            }
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
        // ENG-1608.  We sniff for FailureSiteUpdateMessages here so
        // that a node will participate in the failure resolution protocol
        // even if it hasn't directly witnessed a node fault.
        if (message instanceof FailureSiteUpdateMessage)
        {
            int failed_host_id =
                VoltDB.instance().getCatalogContext().siteTracker.
                getHostForSite(((FailureSiteUpdateMessage)message).m_failedSiteIds.iterator().next());
            VoltDB.instance().getFaultDistributor().
            reportFault(new NodeFailureFault(
                    failed_host_id,
                    VoltDB.instance().getCatalogContext().siteTracker.getNonExecSitesForHost(failed_host_id),
                    m_hostMessenger.getHostnameForHostID(failed_host_id)));
        }
    }
}
