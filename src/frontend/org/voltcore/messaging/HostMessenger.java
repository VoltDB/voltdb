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

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.ZooDefs.Ids;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONObject;
import org.voltcore.VoltDB;
import org.voltcore.agreement.AgreementSite;
import org.voltcore.agreement.InterfaceToMessenger;
import org.voltcore.agreement.ZKUtil;
import org.voltcore.logging.VoltLogger;
import org.voltcore.network.VoltNetworkPool;
import org.voltcore.utils.MiscUtils;

public class HostMessenger implements Messenger, SocketJoiner.JoinHandler, InterfaceToMessenger {

    public static class Config {
        public InetSocketAddress coordinatorIp;
        public String zkInterface = "127.0.0.1:2181";
        public ScheduledExecutorService ses =
            MiscUtils.getScheduledThreadPoolExecutor("HostMessenger SES", 1, 1024 * 128);
        public String internalInterface = "";
        public int internalPort = 3021;
        public int deadHostTimeout = 10000;
        public long backwardsTimeForgivenessWindow = 1000 * 60 * 60 * 24 * 7;
        public VoltMessageFactory factory = new VoltMessageFactory();

        public Config(String coordIp, int coordPort) {
            coordinatorIp = new InetSocketAddress(coordIp, coordPort);
        }

        public Config() {
            this("127.0.0.1", 3021);
        }
    }

    private static final VoltLogger m_logger = new VoltLogger("org.voltdb.messaging.impl.HostMessenger");
    private static final VoltLogger hostLog = new VoltLogger("HOST");

    public static final long AGREEMENT_SITE_ID = -1;

    int m_localHostId;

    private final Config m_config;
    private final SocketJoiner m_joiner;
    private final VoltNetworkPool m_network;

    private volatile boolean m_localhostReady = false;

    final AtomicReference<HashMap<Integer, ForeignHost>> m_foreignHosts =
        new AtomicReference<HashMap<Integer, ForeignHost>>(new HashMap<Integer, ForeignHost>());
    final AtomicReference<HashMap<Long, Mailbox>> m_siteMailboxes =
        new AtomicReference<HashMap<Long, Mailbox>>(new HashMap<Long, Mailbox>());

    private final Set<Integer> m_knownFailedHosts = Collections.synchronizedSet(new HashSet<Integer>());

    private AgreementSite m_agreementSite;
    private ZooKeeper m_zk;
    private final AtomicInteger m_nextSiteId = new AtomicInteger(0);
    public Mailbox getMailbox(long hsId) {
        return m_siteMailboxes.get().get(hsId);
    }

    /**
     *
     * @param network
     * @param coordinatorIp
     * @param expectedHosts
     * @param catalogCRC
     * @param hostLog
     */
    public HostMessenger(
            Config config)
    {
        m_config = config;
        m_network = new VoltNetworkPool( Runtime.getRuntime().availableProcessors() / 2, m_config.ses);
        m_joiner = new SocketJoiner(
                m_config.coordinatorIp,
                m_config.internalInterface,
                m_config.internalPort,
                hostLog,
                this);
    }

    @Override
    public synchronized void reportForeignHostFailed(int hostId) {
        if (m_knownFailedHosts.contains(hostId)) {
            return;
        }
        m_knownFailedHosts.add(hostId);
        long initiatorSiteId = (AGREEMENT_SITE_ID << 32) + hostId;
        removeForeignHost(hostId);
        m_agreementSite.reportFault(initiatorSiteId);
    }

    public void start() throws Exception {
        CountDownLatch zkInitBarrier = new CountDownLatch(1);

        /*
         * If start returns true then this node is the leader, it bound to the coordinator address
         * It needs to bootstrap its agreement site so that other nodes can join
         */
        if(m_joiner.start(zkInitBarrier)) {
            m_network.start();
            m_config.internalInterface = m_config.coordinatorIp.getAddress().getHostAddress();
            m_config.internalPort = m_config.coordinatorIp.getPort();
            long agreementHSId = (AGREEMENT_SITE_ID << 32) + m_localHostId;
            HashSet<Long> agreementSites = new HashSet<Long>();
            agreementSites.add(agreementHSId);
            SiteMailbox sm = new SiteMailbox(this, agreementHSId);
            createMailbox(agreementHSId, sm);
            m_agreementSite =
                new AgreementSite(
                    this,
                    agreementHSId,
                    agreementSites,
                    0,
                    sm,
                    new InetSocketAddress(
                            m_config.zkInterface.split(":")[0],
                            Integer.parseInt(m_config.zkInterface.split(":")[1])),
                    m_config.backwardsTimeForgivenessWindow);
            m_agreementSite.start();
            m_agreementSite.waitForRecovery();
            m_zk = org.voltcore.agreement.ZKUtil.getClient(m_config.zkInterface, 60 * 1000);
            if (m_zk == null) {
                throw new Exception("Timed out trying to connect local ZooKeeper instance");
            }

            final int selectedHostId = selectNewHostId(m_config.coordinatorIp.toString(), true);
            if (selectedHostId != 0) {
                VoltDB.crashLocalVoltDB("Selected host id for coordinator was not 0, " + selectedHostId, false, null);
            }
            m_zk.create("/hosts", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            byte hostInfoBytes[] = m_config.coordinatorIp.toString().getBytes("UTF-8");
            m_zk.create("/hosts/host" + selectedHostId, hostInfoBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        }
        zkInitBarrier.countDown();
    }

    //For test only
    protected HostMessenger() {
        this(new Config());
    }

    /* In production, this is always the network created by VoltDB.
     * Tests, however, can create their own network object. ForeignHost
     * will query HostMessenger for the network to join.
     */
    public VoltNetworkPool getNetwork() {
        return m_network;
    }

    public VoltMessageFactory getMessageFactory()
    {
        return m_config.factory;
    }

    /**
     * Wait until all the nodes have built a mesh.
     */
    public void waitForGroupJoin() {
         waitForGroupJoin(Integer.MAX_VALUE);
    }


    @Override
    public void notifyOfJoin(int hostId, SocketChannel socket, InetSocketAddress listeningAddress) {
        System.out.println(getHostId() + " notified of " + hostId);
        prepSocketChannel(socket);
        ForeignHost fhost = null;
        try {
            fhost = new ForeignHost(this, hostId, socket, m_config.deadHostTimeout, listeningAddress);
            putForeignHost(hostId, fhost);
            fhost.register(this);
            fhost.enableRead();
        } catch (java.io.IOException e) {
            VoltDB.crashLocalVoltDB("", true, e);
        }
    }

    private void prepSocketChannel(SocketChannel sc) {
        try {
            sc.socket().setSendBufferSize(1024*1024*2);
            sc.socket().setReceiveBufferSize(1024*1024*2);
        } catch (SocketException e) {
            e.printStackTrace();
        }
    }

    private void putForeignHost(int hostId, ForeignHost fh) {
        while (true) {
            HashMap<Integer, ForeignHost> original = m_foreignHosts.get();
            HashMap<Integer, ForeignHost> update = new HashMap<Integer, ForeignHost>(original);
            update.put(hostId, fh);
            if (m_foreignHosts.compareAndSet(original, update)) {
                break;
            }
        }
    }

    private void removeForeignHost(int hostId) {
        while (true) {
            HashMap<Integer, ForeignHost> original = m_foreignHosts.get();
            HashMap<Integer, ForeignHost> update = new HashMap<Integer, ForeignHost>(original);
            if (update.remove(hostId) == null) {
                return;
            }
            if (m_foreignHosts.compareAndSet(original, update)) {
                break;
            }
        }
    }

    @Override
    public void requestJoin(SocketChannel socket, InetSocketAddress listeningAddress) throws Exception {
        Integer hostId = selectNewHostId(socket.socket().getInetAddress().getHostAddress(), false);
        prepSocketChannel(socket);
        ForeignHost fhost = null;
        try {
            writeRequestJoinResponse( hostId, socket);
            ByteBuffer finishedJoining = ByteBuffer.allocate(1);
            socket.configureBlocking(false);
            long start = System.currentTimeMillis();
            while (finishedJoining.hasRemaining() && System.currentTimeMillis() - start < 120000) {
                int read = socket.read(finishedJoining);
                if (read == -1) {
                    hostLog.info("New connection was unable to establish mesh");
                    return;
                } else if (read < 1) {
                    Thread.sleep(5);
                }
            }
            fhost = new ForeignHost(this, hostId, socket, m_config.deadHostTimeout, listeningAddress);
            putForeignHost(hostId, fhost);
            fhost.register(this);
            fhost.enableRead();
            if (!m_agreementSite.requestJoin((AGREEMENT_SITE_ID << 32) + hostId).await(60, TimeUnit.SECONDS)) {
                reportForeignHostFailed(hostId);
            }
        } catch (Throwable e) {
            VoltDB.crashLocalVoltDB("", true, e);
        }
    }

    /*
     * Acquire a lock on allocating new host ids, find the next ID to allocate, allocate it
     */
    private Integer selectNewHostId(String address, boolean createPath) throws Exception {
        if (createPath) {
            m_zk.create("/hostids", null,  Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        String node =
            m_zk.create(
                    "/hostids/host", address.getBytes("UTF-8"), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);

        return Integer.valueOf(node.substring(node.length() - 10));
    }

    private void writeRequestJoinResponse(int hostId, SocketChannel socket) throws Exception {
        JSONObject jsObj = new JSONObject();
        jsObj.put("newHostId", hostId);
        jsObj.put("reportedAddress",
                ((InetSocketAddress)socket.socket().getRemoteSocketAddress()).getAddress().getHostAddress());
        JSONArray jsArray = new JSONArray();
        JSONObject hostObj = new JSONObject();
        hostObj.put("hostId", getHostId());
        hostObj.put("address",
                m_config.internalInterface.isEmpty() ? socket.socket().getLocalAddress() : m_config.internalInterface);
        hostObj.put("port", m_config.internalPort);
        jsArray.put(hostObj);
        for (Map.Entry<Integer, ForeignHost>  entry : m_foreignHosts.get().entrySet()) {
            if (entry.getValue() == null) continue;
            int hsId = entry.getKey();
            ForeignHost fh = entry.getValue();
            hostObj = new JSONObject();
            hostObj.put("hostId", hsId);
            hostObj.put("address", fh.m_listeningAddress.getAddress().getHostAddress());
            hostObj.put("port", fh.m_listeningAddress.getPort());
            jsArray.put(hostObj);
        }
        jsObj.put("hosts", jsArray);
        byte messageBytes[] = jsObj.toString(4).getBytes("UTF-8");
        ByteBuffer message = ByteBuffer.allocate(4 + messageBytes.length);
        message.putInt(messageBytes.length);
        message.put(messageBytes).flip();
        while (message.hasRemaining()) {
            socket.write(message);
        }
    }

    @Override
    public void notifyOfHosts(
            int yourHostId,
            int[] hosts,
            SocketChannel[] sockets,
            InetSocketAddress listeningAddresses[]) throws Exception {
        m_localHostId = yourHostId;
        long agreementHSId = (AGREEMENT_SITE_ID << 32) + yourHostId;
        HashSet<Long> agreementSites = new HashSet<Long>();;
        agreementSites.add(agreementHSId);
        m_network.start();//network must be running for register to work
        for (int ii = 0; ii < hosts.length; ii++) {
            System.out.println(yourHostId + " Notified of host " + hosts[ii]);
            agreementSites.add( (AGREEMENT_SITE_ID << 32) + hosts[ii] );
            prepSocketChannel(sockets[ii]);
            ForeignHost fhost = null;
            try {
                fhost = new ForeignHost(this, hosts[ii], sockets[ii], m_config.deadHostTimeout, listeningAddresses[ii]);
                putForeignHost(hosts[ii], fhost);
                fhost.register(this);
            } catch (java.io.IOException e) {
                VoltDB.crashLocalVoltDB("", true, e);
            }
        }

        SiteMailbox sm = new SiteMailbox(this, agreementHSId);
        createMailbox(agreementHSId, sm);
        m_agreementSite =
            new AgreementSite(
                this,
                agreementHSId,
                agreementSites,
                0,
                sm,
                new InetSocketAddress(
                        m_config.zkInterface.split(":")[0],
                        Integer.parseInt(m_config.zkInterface.split(":")[1])),
                m_config.backwardsTimeForgivenessWindow);
        /*
         * Now that the agreement site mailbox has been created it is safe
         * to enable read
         */
        for (ForeignHost fh : m_foreignHosts.get().values()) {
            fh.enableRead();
        }
        m_agreementSite.start();
        m_agreementSite.waitForRecovery();
        m_zk = org.voltcore.agreement.ZKUtil.getClient(m_config.zkInterface, 60 * 1000);
        if (m_zk == null) {
            throw new Exception("Timed out trying to connect local ZooKeeper instance");
        }
        byte hostInfoBytes[];
        if (m_config.internalInterface.isEmpty()) {
            InetAddress intf = InetAddress.getByName(m_joiner.m_reportedInternalInterface);
            InetSocketAddress addr = new InetSocketAddress(intf, m_config.internalPort);
            hostInfoBytes = addr.toString().getBytes("UTF-8");
        } else {
            InetAddress intf = InetAddress.getByName(m_config.internalInterface);
            InetSocketAddress addr = new InetSocketAddress(intf, m_config.internalPort);
            hostInfoBytes = addr.toString().getBytes("UTF-8");
        }
        m_zk.create("/hosts/host" + getHostId(), hostInfoBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    }

    /**
     * Wait until all the nodes have built a mesh.
     */
    public void waitForGroupJoin(int expectedHosts) {
        try {
            while (true) {
                ZKUtil.FutureWatcher fw = new ZKUtil.FutureWatcher();
                if (m_zk.getChildren("/hosts", fw).size() == expectedHosts) {
                    break;
                }
                fw.get();
            }
        } catch (Exception e) {
            VoltDB.crashLocalVoltDB("Error waiting for hosts to be ready", false, e);
        }
    }

    public int getHostId() {
        return m_localHostId;
    }

    public String getHostname() {
        String hostname = org.voltcore.utils.MiscUtils.getHostnameOrAddress();
        return hostname;
    }

    /**
     * Given a hostid, return the hostname for it
     */
    @Override
    public String getHostnameForHostID(int hostId) {
        HashMap<Integer, ForeignHost> foreignHosts = m_foreignHosts.get();
        ForeignHost fh = foreignHosts.get(hostId);
        return fh == null ? "UNKNOWN" : foreignHosts.get(hostId).hostname();
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
    ForeignHost presend(long hsId, VoltMessage message)
    throws MessagingException {
        int hostId = (int)hsId;

        // the local machine case
        if (hostId == m_localHostId) {
            Mailbox mbox = m_siteMailboxes.get().get(hsId);
            if (mbox != null) {
                mbox.deliver(message);
                return null;
            }
        }

        // the foreign machine case
        ForeignHost fhost = m_foreignHosts.get().get(hostId);

        if (fhost == null)
        {
            if (!m_knownFailedHosts.contains(hostId)) {
                throw new MessagingException(
                        "Attempted to send a message to foreign host with id " +
                        hostId + " but there is no such host.");
            }
            return null;
        }

        if (!fhost.isUp())
        {
            //Throwable t = new Throwable();
            //java.io.StringWriter sw = new java.io.StringWriter();
            //java.io.PrintWriter pw = new java.io.PrintWriter(sw);
            //t.printStackTrace(pw);
            //pw.flush();
            m_logger.warn("Attempted delivery of message to failed site: " + MiscUtils.hsIdToString(hsId));
            //m_logger.warn(sw.toString());
            return null;
        }
        return fhost;
    }

    @Override
    public Mailbox createMailbox() {
        final long siteId = m_nextSiteId.getAndIncrement();
        long hsId = getHostId() + (siteId << 32);
        SiteMailbox sm = new SiteMailbox( this, siteId);

        while (true) {
            HashMap<Long, Mailbox> original = m_siteMailboxes.get();
            HashMap<Long, Mailbox> update = new HashMap<Long, Mailbox>(original);
            update.put(hsId, sm);
            if (m_siteMailboxes.compareAndSet(original, update)) {
                break;
            }
        }
        return sm;
    }

    public void send(final long destinationHSId, final VoltMessage message)
    throws MessagingException
    {
        assert(message != null);

        ForeignHost host = presend(destinationHSId, message);
        if (host != null) {
            Long dests[] = {destinationHSId};
            host.send(Arrays.asList(dests), message);
        }
    }

    public void send(long[] destinationHSIds, final VoltMessage message)
            throws MessagingException {

        assert(message != null);
        assert(destinationHSIds != null);
        final HashMap<ForeignHost, ArrayList<Long>> foreignHosts =
            new HashMap<ForeignHost, ArrayList<Long>>(32);
        for (long hsId : destinationHSIds) {
            ForeignHost host = presend(hsId, message);
            if (host == null) continue;
            ArrayList<Long> bundle = foreignHosts.get(host);
            if (bundle == null) {
                bundle = new ArrayList<Long>();
                foreignHosts.put(host, bundle);
            }
            bundle.add(hsId);
        }

        if (foreignHosts.size() == 0) return;

        for (Entry<ForeignHost, ArrayList<Long>> e : foreignHosts.entrySet()) {
                e.getKey().send(e.getValue(), message);
        }
        foreignHosts.clear();
    }

    /**
     * Block on this call until the number of ready hosts is
     * equal to the number of expected hosts.
     *
     * @return True if returning with all hosts ready. False if error.
     */
    public void waitForAllHostsToBeReady(int expectedHosts) {
        m_localhostReady = true;
        try {
            m_zk.create("/ready_hosts", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (Exception e) {
            //can fail
        }
        try {
            m_zk.create("/ready_hosts/host", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
            while (true) {
                ZKUtil.FutureWatcher fw = new ZKUtil.FutureWatcher();
                if (m_zk.getChildren("/ready_hosts", fw).size() == expectedHosts) {
                    break;
                }
                fw.get();
            }
        } catch (Exception e) {
            VoltDB.crashLocalVoltDB("Error waiting for hosts to be ready", false, e);
        }
    }


    public synchronized boolean isLocalHostReady() {
        return m_localhostReady;
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
//        if (m_foreignHosts.length <= hostId)
//            throw new Exception("Rejoin HostId out of expexted range.");
//        SiteTracker st = VoltDB.instance().getCatalogContext().siteTracker;
//        if (m_foreignHosts[hostId] != null && st.getAllLiveHosts().contains(hostId))
//            throw new Exception("Rejoin HostId is not a failed host.");

//        SocketChannel sock = SocketJoiner.connect(
//                m_localHostId, hostId, addr, catalogCRC, deploymentCRC,
//                liveHosts, faultSequenceNumber, catalogVersionNumber,
//                catalogTxnId, catalogBytes);
//
//        m_tempNewFH = new ForeignHost(this, hostId, sock);
//        m_tempNewFH.sendReadyMessage();
//        m_tempNewHostId = hostId;
    }

    public void shutdown() throws InterruptedException
    {
        m_zk.close();
        m_agreementSite.shutdown();
        for (ForeignHost host : m_foreignHosts.get().values())
        {
            // null is OK. It means this host never saw this host id up
            if (host != null)
            {
                host.close();
            }
        }
        m_joiner.shutdown();
    }

    @Override
    public void createMailbox(Long proposedHSId, Mailbox mailbox) {
        long hsId = 0;
        if (proposedHSId != null) {
            if (m_siteMailboxes.get().containsKey(proposedHSId)) {
                VoltDB.crashLocalVoltDB(
                        "Attempted to create a mailbox for site " +
                        MiscUtils.hsIdToString(proposedHSId) + " twice", true, null);
            }
            hsId = proposedHSId;
        } else {
            hsId = getHostId() + (((long)m_nextSiteId.getAndIncrement()) << 32);
            mailbox.setHSId(hsId);
        }

        while (true) {
            HashMap<Long, Mailbox> original = m_siteMailboxes.get();
            HashMap<Long, Mailbox> update = new HashMap<Long, Mailbox>(original);
            update.put(hsId, mailbox);
            if (m_siteMailboxes.compareAndSet(original, update)) {
                break;
            }
        }
    }

    /**
     * Get the number of up foreign hosts. Used for test purposes.
     * @return The number of up foreign hosts.
     */
    public int countForeignHosts() {
        int retval = 0;
        for (ForeignHost host : m_foreignHosts.get().values())
            if ((host != null) && (host.isUp()))
                retval++;
        return retval;
    }

    /**
     * Kill a foreign host socket by id.
     * @param hostId The id of the foreign host to kill.
     */
    public void closeForeignHostSocket(int hostId) {
        ForeignHost fh = m_foreignHosts.get().get(hostId);
        putForeignHost(hostId, null);
        if (fh != null && fh.isUp()) {
            fh.killSocket();
        }
    }

    public ZooKeeper getZK() {
        return m_zk;
    }
}
