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

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Set;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicBoolean;

import org.voltcore.logging.VoltLogger;
import org.voltcore.network.Connection;
import org.voltcore.network.PicoNetwork;
import org.voltcore.network.QueueMonitor;
import org.voltcore.network.VoltProtocolHandler;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.DeferredSerialization;
import org.voltcore.utils.EstTime;
import org.voltdb.OperationMode;
import org.voltdb.VoltDB;

public class Subconnection {
    final PicoNetwork m_network;
    // hold onto the socket so we can kill it
    private final Socket m_socket;
    private final Integer m_hostId;
    private final PicoInputHandler m_handler;
    private final HostMessenger m_hostMessenger;
    private final ForeignHost m_fh;
    private boolean m_isUp;

    // used to immediately cut off reads from a foreign host
    // great way to trigger a heartbeat timout / simulate a network partition
    private AtomicBoolean m_linkCutForTest = new AtomicBoolean(false);

    private static final VoltLogger hostLog = new VoltLogger("HOST");

    /** Intra-cluster implementation of InputHandler */
    public class PicoInputHandler extends VoltProtocolHandler {

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
            m_isUp = false;
            m_fh.connectionStopping(Subconnection.this);
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
                    m_hostId, getHostnameOrIP(), message);
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

        m_fh.updateLastMessageTime(EstTime.currentTimeMillis());
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

    Subconnection(int hostId, HostMessenger hm, ForeignHost fh, SocketChannel socket, PicoNetwork network) {
        m_hostId = hostId;
        m_socket = socket.socket();
        m_network = network;
        m_handler = new PicoInputHandler();
        m_hostMessenger = hm;
        m_fh = fh;
        m_isUp = true;
    }

    public void enableRead(Set<Long> verbotenThreads) {
        m_network.start(m_handler, verbotenThreads);
    }

    void send(final long destinations[], final VoltMessage message) {
        if (!m_isUp) {
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
                        final int len = 4      /* length prefix */
                                + 8            /* source hsid */
                                + 4            /* destinationCount */
                                + 8 * destinations.length  /* destination list */
                                + message.getSerializedSize();
                        return len;
                    }
                });
    }

    synchronized void close() throws InterruptedException {
        m_isUp = false;
        m_network.shutdownAsync();
    }

    /**
     * Used only for test code to kill this FH
     */
    void killSocket() {
        try {
            m_socket.setKeepAlive(false);
            m_socket.setSoLinger(false, 0);
            Thread.sleep(25);
            m_socket.close();
            Thread.sleep(25);
            System.gc();
            Thread.sleep(25);
        } catch (Exception e) {
            // don't REALLY care if this fails
            e.printStackTrace();
        }
    }

    public void sendPoisonPill(String err, int cause) {
     // if this link is "gone silent" for partition tests, just drop the message on the floor
        if (m_linkCutForTest.get()) {
            return;
        }

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
        message.putInt(ForeignHost.POISON_PILL);
        message.putInt(errBytes.length);
        message.put(errBytes);
        message.putInt(cause);
        message.flip();
        m_network.enqueue(message);
    }

    public FutureTask<Void> sendStopNodeNotice(int targetHostId) {
        // if this link is "gone silent" for partition tests, just drop the message on the floor
        if (m_linkCutForTest.get()) {
            return null;
        }
        ByteBuffer message = ByteBuffer.allocate(20);
        message.putInt(message.capacity() - 4);
        message.putLong(-1);
        message.putInt(ForeignHost.STOPNODE_NOTICE);
        message.putInt(targetHostId);
        message.flip();
        return m_network.enqueueAndDrain(message);
    }

    String getHostnameAndIPAndPort() {
        return m_network.getHostnameAndIPAndPort();
    }

    String getHostnameOrIP() {
        return m_network.getHostnameOrIP();
    }

    PicoNetwork getPicoNetwork() {
        return m_network;
    }

    void cutLink() {
        m_linkCutForTest.set(true);
    }

}
