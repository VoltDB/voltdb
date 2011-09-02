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
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicInteger;

import org.voltdb.VoltDB;
import org.voltdb.client.ConnectionUtil;
import org.voltdb.dtxn.SiteTracker;
import org.voltdb.logging.VoltLogger;
import org.voltdb.network.VoltNetwork;
import org.voltdb.utils.DBBPool;
import org.voltdb.utils.DBBPool.BBContainer;
import org.voltdb.utils.DeferredSerialization;

public class HostMessenger implements Messenger {

    private static final VoltLogger m_logger = new VoltLogger("org.voltdb.messaging.impl.HostMessenger");
    static class SerializedMessage {
        public byte[] buf;
        public int off;
        public int len;
    }

    static class ForeignHostBundle {
        int count = 0;
        final int[] siteIds = new int[VoltDB.MAX_SITES_PER_HOST];

        public void add(int siteId) {
            assert(count < (VoltDB.MAX_SITES_PER_HOST - 1));
            siteIds[count++] = siteId;
        }
    }

    int m_localHostId;
    boolean m_initialized = false;

    private final SocketJoiner m_joiner;
    private final VoltNetwork m_network;
    //private final InetAddress m_coordinatorAddr;
    private final int m_expectedHosts;

    final ForeignHost[] m_foreignHosts;
    final MessengerSite[] m_messengerSites;
    int m_largestHostId = 0;
    int m_largestSiteId = 0;

    ForeignHost m_tempNewFH = null;
    int m_tempNewHostId = -1;

    public class JoiningNodeInfo {
        public String hostName;
        public int hostId;
    }

    final AtomicInteger m_hostsToWaitFor = new AtomicInteger();

    /**
     *
     * @param network
     * @param coordinatorIp
     * @param expectedHosts
     * @param catalogCRC
     * @param hostLog
     */
    public HostMessenger(VoltNetwork network, InetAddress coordinatorIp, int expectedHosts,
            long catalogCRC, long deploymentCRC, VoltLogger hostLog)
    {
        m_expectedHosts = expectedHosts;
        m_hostsToWaitFor.set(expectedHosts);
        m_network = network;
        m_joiner = new SocketJoiner(coordinatorIp, m_expectedHosts,
                catalogCRC, deploymentCRC, hostLog);
        m_joiner.start();

        m_foreignHosts = new ForeignHost[expectedHosts + 1];
        m_messengerSites = new MessengerSite[VoltDB.MAX_SITES_PER_HOST + 1];
        m_largestHostId = expectedHosts;
    }

    public HostMessenger(VoltNetwork network, ServerSocketChannel acceptor, int expectedHosts,
            long catalogCRC, long deploymentCRC, VoltLogger hostLog)
    {
        m_expectedHosts = expectedHosts;
        m_hostsToWaitFor.set(expectedHosts);
        m_network = network;
        m_joiner = new SocketJoiner(acceptor, expectedHosts,
                catalogCRC, deploymentCRC, hostLog);
        m_joiner.start();

        m_foreignHosts = new ForeignHost[expectedHosts + 1];
        m_messengerSites = new MessengerSite[VoltDB.MAX_SITES_PER_HOST + 1];
        m_largestHostId = expectedHosts;
    }

    //For test only
    protected HostMessenger() {
        m_messengerSites = null;
        m_foreignHosts = null;
        m_network = null;
        m_expectedHosts = 0;
        m_joiner = null;
    }

    /* In production, this is always the network created by VoltDB.
     * Tests, however, can create their own network object. ForeignHost
     * will query HostMessenger for the network to join.
     */
    public VoltNetwork getNetwork() {
        return m_network;
    }

    /** For rejoin */
    public int getDiscoveredCatalogVersion() {
        return m_joiner.getDiscoveredCatalogVersionId();
    }

    /** For rejoin */
    public long getDiscoveredFaultSequenceNumber() {
        return m_joiner.m_discoveredFaultSequenceNumber;
    }

    /** For rejoin */
    public long getDiscoveredCatalogTxnId() {
        return m_joiner.getDiscoveredCatalogTxnId();
    }

    /**
     * Wait until all the nodes have built a mesh.
     * Return metadata in an object array.
     */
    public synchronized Object[] waitForGroupJoin() {
         return waitForGroupJoin(0);
    }

    /**
     * Wait until all the nodes have built a mesh.
     * Return metadata in an object array.
     */
    public synchronized Object[] waitForGroupJoin(int timeout) {
        // no-op if called from another thread after the first init
        if (!m_initialized) {

            try {
                m_joiner.join(timeout);
                if (!m_joiner.getSuccess()) {
                    String errMsg;
                    if (timeout == 0) {
                        errMsg = "The joiner thread was not successful";
                    } else {
                        errMsg = "Timed out waiting for other nodes to connect. It is safe to retry rejoin.";
                    }
                    VoltDB.crashLocalVoltDB(errMsg, false, null);
                }
                //timeout
                if (m_joiner.isAlive()) {
                    VoltDB.crashLocalVoltDB("SockeJoiner is alive after join call.", true, null);
                }
            } catch (InterruptedException e) {
                VoltDB.crashLocalVoltDB("SockeJoiner was interrupted.", false, e);
            }

            m_localHostId = m_joiner.getLocalHostId();
            Map<Integer, SocketChannel> sockets = m_joiner.getHostsAndSockets();
            for (Entry<Integer,SocketChannel> entry: sockets.entrySet()) {
                Integer hostId = entry.getKey();
                SocketChannel socket = entry.getValue();
                try {
                    socket.socket().setSendBufferSize(1024*1024*2);
                    socket.socket().setReceiveBufferSize(1024*1024*2);
                } catch (SocketException e) {
                    e.printStackTrace();
                }
                ForeignHost fhost = null;
                try {
                    fhost = new ForeignHost(this, hostId, socket);
                } catch (java.io.IOException e) {
                    e.printStackTrace();
                }
                m_foreignHosts[hostId] = fhost;
            }

            m_initialized = true;
        }

        HashSet<Integer> downHosts = new HashSet<Integer>();
        for (int ii = 0; ii < (m_foreignHosts.length - 1); ii++) {
            if (m_foreignHosts[ii] == null && ii != m_localHostId) {
                downHosts.add(ii);
            }
        }

        return new Object[] { m_joiner.m_timestamp, m_joiner.m_addr, downHosts };
    }

    public int getHostId() {
        assert m_initialized;
        return m_localHostId;
    }

    public String getHostname() {
        String hostname = ConnectionUtil.getHostnameOrAddress();
        return hostname;
    }

    MessengerSite getSite(int siteId) {
        assert m_initialized;
        int hostId = siteId / VoltDB.SITES_TO_HOST_DIVISOR;
        if (hostId != m_localHostId)
            return null;
        int localSiteId = siteId % VoltDB.SITES_TO_HOST_DIVISOR;
        return m_messengerSites[localSiteId];
    }

    /**
     * Given a hostid, return the hostname for it
     */
    public String getHostnameForHostID(int hostId) {
        ForeignHost fh = m_foreignHosts[hostId];
        return fh == null ? "UNKNOWN" : m_foreignHosts[hostId].hostname();
    }

    public void createLocalSite(int siteId) {
        assert m_initialized;
        int hostId = siteId / VoltDB.SITES_TO_HOST_DIVISOR;
        int localSiteId = siteId % VoltDB.SITES_TO_HOST_DIVISOR;
        assert(hostId == m_localHostId);
        assert(localSiteId <= VoltDB.MAX_SITES_PER_HOST);
        if (localSiteId > m_largestSiteId)
            m_largestSiteId = localSiteId;


        MessengerSite site = new MessengerSite(this, siteId);
        m_messengerSites[localSiteId] = site;
    }

    /**
     *
     * @param siteId
     * @param mailboxId
     * @param message
     * @return null if message was delivered locally or a ForeignHost
     * reference if a message is read to be delivered remotely.
     * @throws MessagingException
     */
    ForeignHost presend(int siteId, int mailboxId, VoltMessage message)
    throws MessagingException {
        int hostId = siteId / VoltDB.SITES_TO_HOST_DIVISOR;
        int localSiteId = siteId % VoltDB.SITES_TO_HOST_DIVISOR;

        // the local machine case
        if (hostId == m_localHostId) {
            MessengerSite site = m_messengerSites[localSiteId];
            if (site != null) {
                Mailbox mbox = site.getMailbox(mailboxId);
                if (mbox != null) {
                    mbox.deliver(message);
                    return null;
                }
            }
        }

        // the foreign machine case
        ForeignHost fhost = m_foreignHosts[hostId];

        if (fhost == null)
        {
            throw new MessagingException(
                    "Attempted to send a message to foreign host with id " +
                    hostId + " but there is no such host.");
        }

        if (!fhost.isUp())
        {
            //Throwable t = new Throwable();
            //java.io.StringWriter sw = new java.io.StringWriter();
            //java.io.PrintWriter pw = new java.io.PrintWriter(sw);
            //t.printStackTrace(pw);
            //pw.flush();
            m_logger.warn("Attempted delivery of message to failed site: " + siteId);
            //m_logger.warn(sw.toString());
            return null;
        }
        return fhost;
    }

    @Override
    public Mailbox createMailbox(int siteId, int mailboxId, boolean log) {
        assert(m_initialized);
        int localSiteId = siteId % VoltDB.SITES_TO_HOST_DIVISOR;
        MessengerSite site = m_messengerSites[localSiteId];
        if (site == null) return null;

        return site.createMailbox(mailboxId, log);
    }

    public void send(final int siteId, final int mailboxId, final VoltMessage message)
    throws MessagingException
    {
        assert(m_initialized);
        assert(message != null);

        ForeignHost host = presend(siteId, mailboxId, message);
        if (host != null) {
            int dests[] = {siteId};
            host.send(mailboxId, dests, 1,
                    new DeferredSerialization() {
                @Override
                public final BBContainer serialize(final DBBPool pool) throws IOException {
                    return message.getBufferForMessaging(pool);
                }

                @Override
                public final void cancel() {
                    message.discard();
                }
            });
        }
    }

    /*
     * Will always allocate non pooled heap byte buffers
     */
    private final DBBPool heapPool = new DBBPool(true, false);
    public void send(int[] siteIds, int mailboxId, final VoltMessage message)
            throws MessagingException {

        assert(m_initialized);
        assert(message != null);
        assert(siteIds != null);
        final HashMap<ForeignHost, ForeignHostBundle> foreignHosts =
            new HashMap<ForeignHost, ForeignHostBundle>(32);
        for (int siteId : siteIds) {
            ForeignHost host = presend(siteId, mailboxId, message);
            if (host == null) continue;
            ForeignHostBundle bundle = foreignHosts.get(host);
            if (bundle == null) {
                bundle = new ForeignHostBundle();
                foreignHosts.put(host, bundle);
            }
            bundle.add(siteId);
        }

        if (foreignHosts.size() == 0) return;

        /*
         * Defer the serialization of the message to a FutureTask
         * that can be invoked later in the DeferredSerialization.
         * Safe to invoke multiple times and the computation is only
         * done once.
         */
        final FutureTask<ByteBuffer> buildMessageTask = new FutureTask<ByteBuffer>(
                new Callable<ByteBuffer>() {
                    @Override
                    public final ByteBuffer call() throws IOException {
                        return message.getBufferForMessaging(heapPool).b;
                    }
                });

        for (Entry<ForeignHost, ForeignHostBundle> e : foreignHosts.entrySet()) {
                e.getKey().send(mailboxId, e.getValue().siteIds, e.getValue().count,
                        new DeferredSerialization() {
                    @Override
                    public final BBContainer serialize(DBBPool pool) throws IOException {
                        ByteBuffer messageBytes = null;
                        /*
                         * FutureTask will ensure that the task is only run once.
                         */
                        try {
                            buildMessageTask.run();
                            messageBytes = buildMessageTask.get();
                        } catch (InterruptedException e) {
                            m_logger.fatal("Should not be interrupted while serializing messages", e);
                            throw new IOException(e);
                        } catch (ExecutionException e) {
                            if (e.getCause() instanceof IOException) {
                                throw (IOException)e.getCause();
                            } else {
                                m_logger.fatal("Error while serializing message", e);
                                throw new IOException(e);
                            }
                        }

                        /*
                         * Since messageBytes is shared a duplicate view must be made before manipulating the
                         * position, limit, etc. It would be good to only do this copy once and then reference count,
                         * but this isn't the fast path anyways.
                         */
                        ByteBuffer view = messageBytes.duplicate();
                        view.position(0);
                        BBContainer stupidCopy = pool.acquire(view.remaining());
                        stupidCopy.b.put(view);
                        stupidCopy.b.flip();
                        return stupidCopy;
                    }

                    @Override
                    public final void cancel() {

                    }
                });
        }
        foreignHosts.clear();
    }

    /**
     * Send a message to all hosts (and notify this one) that this
     * host is ready.
     */
    public void sendReadyMessage() {
        hostIsReady(m_localHostId);
        for (ForeignHost host : m_foreignHosts)
            if (host != null)
                host.sendReadyMessage();
    }

    /**
     * Send a catalog (as a byte array) to all known foreign hosts.
     * Uses a special negative mailbox id to bypass the mailbox system.
     */
    public void sendCatalog(byte[] catalogBytes) {
        VoltDB.instance().writeNetworkCatalogToTmp(catalogBytes);
        for (ForeignHost host : m_foreignHosts) {
            if (host != null) {
                host.sendBytesToMailbox(ForeignHost.CATALOG_SIGNAL, catalogBytes);
            }
        }
    }

    /**
     * Tell other nodes to die, with a handy message.
     * Uses a special negative mailbox id to bypass the mailbox system.
     */
    public void sendPoisonPill(String msg) {
        byte[] msgBytes = new byte[0];
        try {
            if (msg != null) {
                msgBytes = msg.getBytes("UTF-8");
            }
        } catch (UnsupportedEncodingException e) {
            VoltDB.crashLocalVoltDB("Missing UTF-8 encoding. Broken JVM.", false, e);
        }

        for (ForeignHost host : m_foreignHosts) {
            if (host != null) {
                host.sendBytesToMailbox(ForeignHost.POISON_SIGNAL, msgBytes);
            }
        }
    }

    /**
     * Block on this call until the number of ready hosts is
     * equal to the number of expected hosts.
     *
     * @return True if returning with all hosts ready. False if error.
     */
    public boolean waitForAllHostsToBeReady() {
        while (m_hostsToWaitFor.get() > 0)
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                assert(false);
                return false;
            }
        return true;
    }

    /**
     * Initiate the addition of a replacement foreign host.
     *
     * @param hostId The id of the failed host to replace.
     * @param sock A network connection to that host.
     * @throws Exception Throws exceptions on failure.
     */
    public void rejoinForeignHostPrepare(int hostId,
                                         InetSocketAddress addr,
                                         long catalogCRC,
                                         long deploymentCRC,
                                         Set<Integer> liveHosts,
                                         long faultSequenceNumber,
                                         int catalogVersionNumber,
                                         long catalogTxnId,
                                         byte[] catalogBytes) throws Exception {
        if (hostId < 0)
            throw new Exception("Rejoin HostId can be negative.");
        if (m_foreignHosts.length <= hostId)
            throw new Exception("Rejoin HostId out of expexted range.");
        SiteTracker st = VoltDB.instance().getCatalogContext().siteTracker;
        if (m_foreignHosts[hostId] != null && st.getAllLiveHosts().contains(hostId))
            throw new Exception("Rejoin HostId is not a failed host.");

        SocketChannel sock = SocketJoiner.connect(
                m_localHostId, hostId, addr, catalogCRC, deploymentCRC,
                liveHosts, faultSequenceNumber, catalogVersionNumber,
                catalogTxnId, catalogBytes);

        m_tempNewFH = new ForeignHost(this, hostId, sock);
        m_tempNewFH.sendReadyMessage();
        m_tempNewHostId = hostId;
    }

    /**
     * Finish joining the network.
     */
    public JoiningNodeInfo rejoinForeignHostCommit() {
        assert(m_tempNewFH != null);
        assert(m_tempNewHostId >= 0);

        m_foreignHosts[m_tempNewHostId] = m_tempNewFH;

        JoiningNodeInfo retval = new JoiningNodeInfo();
        retval.hostId = m_tempNewHostId;
        retval.hostName = m_tempNewFH.inetAddrString();

        m_tempNewFH = null;
        m_tempNewHostId = -1;

        assert(retval.hostId >= 0);
        assert(retval.hostName != null);
        return retval;
    }

    /**
     * Reverse any changes made while adding a foreign host.
     * This probably isn't strictly necessary, but if more
     * functionality is added, this will be nice to have.
     */
    public void rejoinForeignHostRollback() {
        if (m_tempNewFH != null) {
            m_tempNewFH.close();
            m_tempNewFH = null;
        }
        m_tempNewHostId = -1;
    }

    public void shutdown()
    {
        for (ForeignHost host : m_foreignHosts)
        {
            // null is OK. It means this host never saw this host id up
            if (host != null)
            {
                host.close();
            }
        }
    }

    /**
     * Tell the system a host is ready, including the local host.
     *
     * @param hostId The id of the host that is ready.
     */
    synchronized void hostIsReady(int hostId) {
        m_hostsToWaitFor.decrementAndGet();
    }

    @Override
    public void createMailbox(int siteId, int mailboxId, Mailbox mailbox) {
        assert(m_initialized);
        int localSiteId = siteId % VoltDB.SITES_TO_HOST_DIVISOR;
        MessengerSite site = m_messengerSites[localSiteId];
        if (site == null) throw new IllegalStateException("No messenger site for siteId " + siteId);
        site.createMailbox(mailboxId, mailbox);
    }

    /**
     * Get the number of up foreign hosts. Used for test purposes.
     * @return The number of up foreign hosts.
     */
    public int countForeignHosts() {
        int retval = 0;
        for (ForeignHost host : m_foreignHosts)
            if ((host != null) && (host.isUp()))
                retval++;
        return retval;
    }

    /**
     * Kill a foreign host socket by id.
     * @param hostId The id of the foreign host to kill.
     */
    public void closeForeignHostSocket(int hostId) {
        for (int i = 0; i < m_foreignHosts.length; i++) {
            ForeignHost host = m_foreignHosts[i];
            if ((host != null) && (host.isUp())) {
                if (i == hostId) {
                    host.killSocket();
                    return;
                }
            }
        }
    }
}
