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

import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.ZooDefs.Ids;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltcore.agreement.AgreementSite;
import org.voltcore.agreement.InterfaceToMessenger;
import org.voltcore.logging.VoltLogger;
import org.voltcore.network.VoltNetworkPool;
import org.voltcore.utils.COWMap;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.InstanceId;
import org.voltcore.utils.PortGenerator;
import org.voltcore.zk.CoreZK;
import org.voltcore.zk.ZKUtil;
import org.voltdb.VoltDB;
import org.voltdb.utils.MiscUtils;

/**
 * Host messenger contains all the code necessary to join a cluster mesh, and create mailboxes
 * that are addressable from anywhere within that mesh. Host messenger also provides
 * a ZooKeeper instance that is maintained within the mesh that can be used for distributed coordination
 * and failure detection.
 */
public class HostMessenger implements SocketJoiner.JoinHandler, InterfaceToMessenger {
    private static final VoltLogger logger = new VoltLogger("NETWORK");

    /**
     * Configuration for a host messenger. The leader binds to the coordinator ip and
     * not the internal interface or port. Nodes that fail to become the leader will
     * connect to the leader using any interface, and will then advertise using the specified
     * internal interface/port.
     *
     * By default all interfaces are used, if one is specified then only that interface will be used.
     *
     */
    public static class Config {
        public InetSocketAddress coordinatorIp;
        public String zkInterface = "127.0.0.1:2181";
        public String internalInterface = "";
        public int internalPort = 3021;
        public int deadHostTimeout = 10000;
        public long backwardsTimeForgivenessWindow = 1000 * 60 * 60 * 24 * 7;
        public VoltMessageFactory factory = new VoltMessageFactory();
        public int networkThreads =  Math.max(2, CoreUtils.availableProcessors() / 4);
        public Queue<String> coreBindIds;;

        public Config(String coordIp, int coordPort) {
            if (coordIp == null || coordIp.length() == 0) {
                coordinatorIp = new InetSocketAddress(coordPort);
            } else {
                coordinatorIp = new InetSocketAddress(coordIp, coordPort);
            }
            initNetworkThreads();
        }

        public Config() {
            this(null, 3021);
        }

        public Config(PortGenerator ports) {
            this(null, 3021);
            zkInterface = "127.0.0.1:" + ports.next();
            internalPort = ports.next();
        }

        public int getZKPort() {
            return MiscUtils.getPortFromHostnameColonPort(zkInterface, VoltDB.DEFAULT_ZK_PORT);
        }

        private void initNetworkThreads() {
            try {
                logger.info("Default network thread count: " + this.networkThreads);
                Integer networkThreadConfig = Integer.getInteger("networkThreads");
                if ( networkThreadConfig != null ) {
                    this.networkThreads = networkThreadConfig;
                    logger.info("Overridden network thread count: " + this.networkThreads);
                }

            } catch (Exception e) {
                logger.error("Error setting network thread count", e);
            }
        }

        @Override
        public String toString() {
            JSONStringer js = new JSONStringer();
            try {
                js.object();
                js.key("coordinatorip").value(coordinatorIp.toString());
                js.key("zkinterface").value(zkInterface);
                js.key("internalinterface").value(internalInterface);
                js.key("internalport").value(internalPort);
                js.key("deadhosttimeout").value(deadHostTimeout);
                js.key("backwardstimeforgivenesswindow").value(backwardsTimeForgivenessWindow);
                js.key("networkThreads").value(networkThreads);
                js.endObject();

                return js.toString();
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static final VoltLogger m_logger = new VoltLogger("org.voltdb.messaging.impl.HostMessenger");
    private static final VoltLogger hostLog = new VoltLogger("HOST");

    public static final int AGREEMENT_SITE_ID = -1;
    public static final int STATS_SITE_ID = -2;
    public static final int ASYNC_COMPILER_SITE_ID = -3;
    public static final int CLIENT_INTERFACE_SITE_ID = -4;

    // we should never hand out this site ID.  Use it as an empty message destination
    public static final int VALHALLA = Integer.MIN_VALUE;

    int m_localHostId;

    private final Config m_config;
    private final SocketJoiner m_joiner;
    private final VoltNetworkPool m_network;
    private volatile boolean m_localhostReady = false;
    // memoized InstanceId
    private InstanceId m_instanceId = null;

    /*
     * References to other hosts in the mesh.
     * Updates via COW
     */
    final COWMap<Integer, ForeignHost> m_foreignHosts = new COWMap<Integer, ForeignHost>();

    /*
     * References to all the local mailboxes
     * Updates via COW
     */
    final COWMap<Long, Mailbox> m_siteMailboxes = new COWMap<Long, Mailbox>();

    /*
     * All failed hosts that have ever been seen.
     * Used to dedupe failures so that they are only processed once.
     */
    private final Set<Integer> m_knownFailedHosts = Collections.synchronizedSet(new HashSet<Integer>());

    private AgreementSite m_agreementSite;
    private ZooKeeper m_zk;
    private final AtomicInteger m_nextSiteId = new AtomicInteger(0);

    public Mailbox getMailbox(long hsId) {
        return m_siteMailboxes.get(hsId);
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
        m_network = new VoltNetworkPool( m_config.networkThreads, m_config.coreBindIds);
        m_joiner = new SocketJoiner(
                m_config.coordinatorIp,
                m_config.internalInterface,
                m_config.internalPort,
                this);
    }

    /**
     * Synchronization protects m_knownFailedHosts and ensures that every failed host is only reported
     * once
     */
    @Override
    public synchronized void reportForeignHostFailed(int hostId) {
        if (m_knownFailedHosts.contains(hostId)) {
            return;
        }
        m_knownFailedHosts.add(hostId);
        long initiatorSiteId = CoreUtils.getHSIdFromHostAndSite(hostId, AGREEMENT_SITE_ID);
        removeForeignHost(hostId);
        m_agreementSite.reportFault(initiatorSiteId);
    }

    /**
     * Start the host messenger and connect to the leader, or become the leader
     * if necessary.
     */
    public void start() throws Exception {
        /*
         * SJ uses this barrier if this node becomes the leader to know when ZooKeeper
         * has been finished bootstrapping.
         */
        CountDownLatch zkInitBarrier = new CountDownLatch(1);

        /*
         * If start returns true then this node is the leader, it bound to the coordinator address
         * It needs to bootstrap its agreement site so that other nodes can join
         */
        if(m_joiner.start(zkInitBarrier)) {
            m_network.start();

            /*
             * m_localHostId is 0 of course.
             */
            long agreementHSId = getHSIdForLocalSite(AGREEMENT_SITE_ID);

            /*
             * A set containing just the leader (this node)
             */
            HashSet<Long> agreementSites = new HashSet<Long>();
            agreementSites.add(agreementHSId);

            /*
             * A basic site mailbox for the agreement site
             */
            SiteMailbox sm = new SiteMailbox(this, agreementHSId);
            createMailbox(agreementHSId, sm);


            /*
             * Construct the site with just this node
             */
            m_agreementSite =
                new AgreementSite(
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
            m_zk = org.voltcore.zk.ZKUtil.getClient(m_config.zkInterface, 60 * 1000);
            if (m_zk == null) {
                throw new Exception("Timed out trying to connect local ZooKeeper instance");
            }

            CoreZK.createHierarchy(m_zk);

            /*
             * This creates the ephemeral sequential node with host id 0 which
             * this node already used for itself. Just recording that fact.
             */
            final int selectedHostId = selectNewHostId(m_config.coordinatorIp.toString());
            if (selectedHostId != 0) {
                org.voltdb.VoltDB.crashLocalVoltDB("Selected host id for coordinator was not 0, " + selectedHostId, false, null);
            }

            // Store the components of the instance ID in ZK
            JSONObject instance_id = new JSONObject();
            instance_id.put("coord",
                    ByteBuffer.wrap(m_config.coordinatorIp.getAddress().getAddress()).getInt());
            instance_id.put("timestamp", System.currentTimeMillis());
            hostLog.debug("Cluster will have instance ID:\n" + instance_id.toString(4));
            byte[] payload = instance_id.toString(4).getBytes("UTF-8");
            m_zk.create(CoreZK.instance_id, payload, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

            /*
             * Store all the hosts and host ids here so that waitForGroupJoin
             * knows the size of the mesh. This part only registers this host
             */
            byte hostInfoBytes[] = m_config.coordinatorIp.toString().getBytes("UTF-8");
            m_zk.create(CoreZK.hosts_host + selectedHostId, hostInfoBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        }
        zkInitBarrier.countDown();
    }

    //For test only
    protected HostMessenger() {
        this(new Config());
    }

    /*
     * The network is only available after start() finishes
     */
    public VoltNetworkPool getNetwork() {
        return m_network;
    }

    public VoltMessageFactory getMessageFactory()
    {
        return m_config.factory;
    }

    /**
     * Get a unique ID for this cluster
     * @return
     */
    public InstanceId getInstanceId()
    {
        if (m_instanceId == null)
        {
            try
            {
                byte[] data =
                    m_zk.getData(CoreZK.instance_id, false, null);
                JSONObject idJSON = new JSONObject(new String(data, "UTF-8"));
                m_instanceId = new InstanceId(idJSON.getInt("coord"),
                        idJSON.getLong("timestamp"));

            }
            catch (Exception e)
            {
                String msg = "Unable to get instance ID info from " + CoreZK.instance_id;
                hostLog.error(msg);
                throw new RuntimeException(msg, e);
            }
        }
        return m_instanceId;
    }

    /*
     * Take the new connection (member of the mesh) and create a foreign host for it
     * and put it in the map of foreign hosts
     */
    @Override
    public void notifyOfJoin(int hostId, SocketChannel socket, InetSocketAddress listeningAddress) {
        System.out.println(getHostId() + " notified of " + hostId);
        prepSocketChannel(socket);
        ForeignHost fhost = null;
        try {
            fhost = new ForeignHost(this, hostId, socket, m_config.deadHostTimeout, listeningAddress);
            fhost.register(this);
            putForeignHost(hostId, fhost);
            fhost.enableRead();
        } catch (java.io.IOException e) {
            org.voltdb.VoltDB.crashLocalVoltDB("", true, e);
        }
    }

    /*
     * Set all the default options for sockets
     */
    private void prepSocketChannel(SocketChannel sc) {
        try {
            sc.socket().setSendBufferSize(1024*1024*2);
            sc.socket().setReceiveBufferSize(1024*1024*2);
        } catch (SocketException e) {
            e.printStackTrace();
        }
    }

    /*
     * Convenience method for doing the verbose COW insert into the map
     */
    private void putForeignHost(int hostId, ForeignHost fh) {
        m_foreignHosts.put(hostId, fh);
    }

    /*
     * Convenience method for doing the verbose COW remove from the map
     */
    private void removeForeignHost(int hostId) {
        ForeignHost fh = m_foreignHosts.remove(hostId);
        if (fh != null) {
            fh.close();
        }
    }

    /*
     * Any node can serve a request to join. The coordination of generating a new host id
     * is done via ZK
     */
    @Override
    public void requestJoin(SocketChannel socket, InetSocketAddress listeningAddress) throws Exception {
        /*
         * Generate the host id via creating an ephemeral sequential node
         */
        Integer hostId = selectNewHostId(socket.socket().getInetAddress().getHostAddress());
        prepSocketChannel(socket);
        ForeignHost fhost = null;
        try {
            try {
                /*
                 * Write the response that advertises the cluster topology
                 */
                writeRequestJoinResponse( hostId, socket);

                /*
                 * Wait for the a response from the joining node saying that it connected
                 * to all the nodes we just advertised. Use a timeout so that the cluster can't be stuck
                 * on failed joins.
                 */
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

                /*
                 * Now add the host to the mailbox system
                 */
                fhost = new ForeignHost(this, hostId, socket, m_config.deadHostTimeout, listeningAddress);
                fhost.register(this);
                putForeignHost(hostId, fhost);
                fhost.enableRead();
            } catch (Exception e) {
                logger.error("Error joining new node", e);
                m_knownFailedHosts.add(hostId);
                removeForeignHost(hostId);
                return;
            }

            /*
             * And the last step is to wait for the new node to join ZooKeeper.
             * This node is the one to create the txn that will add the new host to the list of hosts
             * with agreement sites across the cluster.
             */
            long hsId = CoreUtils.getHSIdFromHostAndSite(hostId, AGREEMENT_SITE_ID);
            if (!m_agreementSite.requestJoin(hsId).await(60, TimeUnit.SECONDS)) {
                reportForeignHostFailed(hostId);
            }
        } catch (Throwable e) {
            org.voltdb.VoltDB.crashLocalVoltDB("", true, e);
        }
    }

    /*
     * Generate a new host id by creating a persistent sequential node
     */
    private Integer selectNewHostId(String address) throws Exception {
        String node =
            m_zk.create(CoreZK.hostids_host,
                    address.getBytes("UTF-8"), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);

        return Integer.valueOf(node.substring(node.length() - 10));
    }

    /*
     * Advertise to a newly connecting node the topology of the cluster so that it can connect to
     * the rest of the nodes
     */
    private void writeRequestJoinResponse(int hostId, SocketChannel socket) throws Exception {
        JSONObject jsObj = new JSONObject();

        /*
         * Tell the new node what its host id is
         */
        jsObj.put("newHostId", hostId);

        /*
         * Echo back the address that the node connected from
         */
        jsObj.put("reportedAddress",
                ((InetSocketAddress)socket.socket().getRemoteSocketAddress()).getAddress().getHostAddress());

        /*
         * Create an array containing an ad for every node including this one
         * even though the connection has already been made
         */
        JSONArray jsArray = new JSONArray();
        JSONObject hostObj = new JSONObject();
        hostObj.put("hostId", getHostId());
        hostObj.put("address",
                m_config.internalInterface.isEmpty() ?
                        socket.socket().getLocalAddress().getHostAddress() : m_config.internalInterface);
        hostObj.put("port", m_config.internalPort);
        jsArray.put(hostObj);
        for (Map.Entry<Integer, ForeignHost>  entry : m_foreignHosts.entrySet()) {
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

    /*
     * SJ invokes this method after a node finishes connecting to the entire cluster.
     * This method constructs all the hosts and puts them in the map
     */
    @Override
    public void notifyOfHosts(
            int yourHostId,
            int[] hosts,
            SocketChannel[] sockets,
            InetSocketAddress listeningAddresses[]) throws Exception {
        m_localHostId = yourHostId;
        long agreementHSId = getHSIdForLocalSite(AGREEMENT_SITE_ID);

        /*
         * Construct the set of agreement sites based on all the hosts that are connected
         */
        HashSet<Long> agreementSites = new HashSet<Long>();
        agreementSites.add(agreementHSId);

        m_network.start();//network must be running for register to work

        for (int ii = 0; ii < hosts.length; ii++) {
            System.out.println(yourHostId + " Notified of host " + hosts[ii]);
            agreementSites.add(CoreUtils.getHSIdFromHostAndSite(hosts[ii], AGREEMENT_SITE_ID));
            prepSocketChannel(sockets[ii]);
            ForeignHost fhost = null;
            try {
                fhost = new ForeignHost(this, hosts[ii], sockets[ii], m_config.deadHostTimeout, listeningAddresses[ii]);
                fhost.register(this);
                putForeignHost(hosts[ii], fhost);
            } catch (java.io.IOException e) {
                org.voltdb.VoltDB.crashLocalVoltDB("", true, e);
            }
        }

        /*
         * Create the local agreement site. It knows that it is recovering because the number of
         * prexisting sites is > 0
         */
        SiteMailbox sm = new SiteMailbox(this, agreementHSId);
        createMailbox(agreementHSId, sm);
        m_agreementSite =
            new AgreementSite(
                    agreementHSId,
                    agreementSites,
                    yourHostId,
                    sm,
                    new InetSocketAddress(
                            m_config.zkInterface.split(":")[0],
                            Integer.parseInt(m_config.zkInterface.split(":")[1])),
                            m_config.backwardsTimeForgivenessWindow);

        /*
         * Now that the agreement site mailbox has been created it is safe
         * to enable read
         */
        for (ForeignHost fh : m_foreignHosts.values()) {
            fh.enableRead();
        }
        m_agreementSite.start();

        /*
         * Do the usual thing of waiting for the agreement site
         * to join the cluster and creating the client
         */
        m_agreementSite.waitForRecovery();
        m_zk = org.voltcore.zk.ZKUtil.getClient(m_config.zkInterface, 60 * 1000);
        if (m_zk == null) {
            throw new Exception("Timed out trying to connect local ZooKeeper instance");
        }

        /*
         * Publish the address of this node to ZK as seen by the leader
         * Also allows waitForGroupJoin to know the number of nodes in the cluster
         */
        byte hostInfoBytes[];
        if (m_config.internalInterface.isEmpty()) {
            InetSocketAddress addr =
                new InetSocketAddress(m_joiner.m_reportedInternalInterface, m_config.internalPort);
            hostInfoBytes = addr.toString().getBytes("UTF-8");
        } else {
            InetSocketAddress addr =
                new InetSocketAddress(m_config.internalInterface, m_config.internalPort);
            hostInfoBytes = addr.toString().getBytes("UTF-8");
        }
        m_zk.create(CoreZK.hosts_host + getHostId(), hostInfoBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    }

    /**
     * Wait until all the nodes have built a mesh.
     */
    public void waitForGroupJoin(int expectedHosts) {
        try {
            while (true) {
                ZKUtil.FutureWatcher fw = new ZKUtil.FutureWatcher();
                final int numChildren = m_zk.getChildren(CoreZK.hosts, fw).size();

                /*
                 * If the target number of hosts has been reached
                 * break out
                 */
                if ( numChildren == expectedHosts) {
                    break;
                }


                /*
                 * If there are extra hosts that means too many Volt procs were started.
                 * Kill this node based on the assumption that we are the extra one. In most
                 * cases this is correct and fine and in the worst case the cluster will hang coming up
                 * because two or more hosts killed themselves
                 */
                if ( numChildren > expectedHosts) {
                    org.voltdb.VoltDB.crashLocalVoltDB("Expected to find " + expectedHosts +
                            " hosts in cluster at startup but found " + numChildren +
                            ".  Terminating this host.", false, null);
                }
                fw.get();
            }
        } catch (Exception e) {
            org.voltdb.VoltDB.crashLocalVoltDB("Error waiting for hosts to be ready", false, e);
        }
    }

    public int getHostId() {
        return m_localHostId;
    }

    public long getHSIdForLocalSite(int site) {
        return CoreUtils.getHSIdFromHostAndSite(getHostId(), site);
    }

    public String getHostname() {
        String hostname = org.voltcore.utils.CoreUtils.getHostnameOrAddress();
        return hostname;
    }

    public List<Integer> getLiveHostIds() throws KeeperException, InterruptedException
    {
        List<Integer> hostids = new ArrayList<Integer>();

        for (String host : m_zk.getChildren(CoreZK.hosts, false, null))
        {
            hostids.add(Integer.parseInt(host.substring(host.indexOf("host") + "host".length())));
        }
        return hostids;
    }

    /**
     * Given a hostid, return the hostname for it
     */
    @Override
    public String getHostnameForHostID(int hostId) {
        ForeignHost fh = m_foreignHosts.get(hostId);
        return fh == null ? "UNKNOWN" : fh.hostname();
    }

    /**
     *
     * @param siteId
     * @param mailboxId
     * @param message
     * @return null if message was delivered locally or a ForeignHost
     * reference if a message is read to be delivered remotely.
     */
    ForeignHost presend(long hsId, VoltMessage message)
    {
        int hostId = (int)hsId;

        // the local machine case
        if (hostId == m_localHostId) {
            Mailbox mbox = m_siteMailboxes.get(hsId);
            if (mbox != null) {
                mbox.deliver(message);
                return null;
            } else {
                hostLog.warn("Mailbox is not registered for site id " + CoreUtils.getSiteIdFromHSId(hsId));
                return null;
            }
        }

        // the foreign machine case
        ForeignHost fhost = m_foreignHosts.get(hostId);

        if (fhost == null)
        {
            if (!m_knownFailedHosts.contains(hostId)) {
                hostLog.warn(
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
            m_logger.warn("Attempted delivery of message to failed site: " + CoreUtils.hsIdToString(hsId));
            //m_logger.warn(sw.toString());
            return null;
        }
        return fhost;
    }

    public void registerMailbox(Mailbox mailbox) {
        if (!m_siteMailboxes.containsKey(mailbox.getHSId())) {
                throw new RuntimeException("Can only register a mailbox with an hsid alreadly generated");
        }
        m_siteMailboxes.put(mailbox.getHSId(), mailbox);
    }

    /*
     * Generate a slot for the mailbox and put a noop box there. Can also
     * supply a value
     */
    public long generateMailboxId(Long mailboxId) {
        final long hsId = mailboxId == null ? getHSIdForLocalSite(m_nextSiteId.getAndIncrement()) : mailboxId;
        m_siteMailboxes.put(hsId, new Mailbox() {
            @Override
            public void send(long hsId, VoltMessage message) {}
            @Override
            public void send(long[] hsIds, VoltMessage message) {}
            @Override
            public void deliver(VoltMessage message) {
                hostLog.info("No-op mailbox(" + CoreUtils.hsIdToString(hsId) + ") dropped message " + message);
            }
            @Override
            public void deliverFront(VoltMessage message) {}
            @Override
            public VoltMessage recv() {return null;}
            @Override
            public VoltMessage recvBlocking() {return null;}
            @Override
            public VoltMessage recvBlocking(long timeout) {return null;}
            @Override
            public VoltMessage recv(Subject[] s) {return null;}
            @Override
            public VoltMessage recvBlocking(Subject[] s) {return null;}
            @Override
            public VoltMessage recvBlocking(Subject[] s, long timeout) { return null;}
            @Override
            public long getHSId() {return 0L;}
            @Override
            public void setHSId(long hsId) {}

        });
        return hsId;
    }

    /*
     * Create a site mailbox with a generated host id
     */
    public Mailbox createMailbox() {
        final int siteId = m_nextSiteId.getAndIncrement();
        long hsId = getHSIdForLocalSite(siteId);
        SiteMailbox sm = new SiteMailbox( this, hsId);
        m_siteMailboxes.put(hsId, sm);
        return sm;
    }

    /**
     * Discard a mailbox
     */
    public void removeMailbox(long hsId) {
        m_siteMailboxes.remove(hsId);
    }

    public void send(final long destinationHSId, final VoltMessage message)
    {
        assert(message != null);

        ForeignHost host = presend(destinationHSId, message);
        if (host != null) {
            Long dests[] = {destinationHSId};
            host.send(Arrays.asList(dests), message);
        }
    }

    public void send(long[] destinationHSIds, final VoltMessage message)
    {
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
            m_zk.create(CoreZK.readyhosts_host, null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            while (true) {
                ZKUtil.FutureWatcher fw = new ZKUtil.FutureWatcher();
                if (m_zk.getChildren(CoreZK.readyhosts, fw).size() == expectedHosts) {
                    break;
                }
                fw.get();
            }
        } catch (Exception e) {
            org.voltdb.VoltDB.crashLocalVoltDB("Error waiting for hosts to be ready", false, e);
        }
    }

    public synchronized boolean isLocalHostReady() {
        return m_localhostReady;
    }


    public void shutdown() throws InterruptedException
    {
        m_zk.close();
        m_agreementSite.shutdown();
        for (ForeignHost host : m_foreignHosts.values())
        {
            // null is OK. It means this host never saw this host id up
            if (host != null)
            {
                host.close();
            }
        }
        m_joiner.shutdown();
        m_network.shutdown();
    }

    /*
     * Register a custom mailbox, optinally specifying what the hsid should be.
     */
    public void createMailbox(Long proposedHSId, Mailbox mailbox) {
        long hsId = 0;
        if (proposedHSId != null) {
            if (m_siteMailboxes.containsKey(proposedHSId)) {
                org.voltdb.VoltDB.crashLocalVoltDB(
                        "Attempted to create a mailbox for site " +
                        CoreUtils.hsIdToString(proposedHSId) + " twice", true, null);
            }
            hsId = proposedHSId;
        } else {
            hsId = getHSIdForLocalSite(m_nextSiteId.getAndIncrement());
            mailbox.setHSId(hsId);
        }

        m_siteMailboxes.put(hsId, mailbox);
    }

    /**
     * Get the number of up foreign hosts. Used for test purposes.
     * @return The number of up foreign hosts.
     */
    public int countForeignHosts() {
        int retval = 0;
        for (ForeignHost host : m_foreignHosts.values())
            if ((host != null) && (host.isUp()))
                retval++;
        return retval;
    }

    /**
     * Kill a foreign host socket by id.
     * @param hostId The id of the foreign host to kill.
     */
    public void closeForeignHostSocket(int hostId) {
        ForeignHost fh = m_foreignHosts.get(hostId);
        if (fh != null && fh.isUp()) {
            fh.killSocket();
        }
        reportForeignHostFailed(hostId);
    }

    public ZooKeeper getZK() {
        return m_zk;
    }

    public void sendPoisonPill(String err) {
        for (ForeignHost fh : m_foreignHosts.values()) {
            if (fh != null && fh.isUp()) {
                fh.sendPoisonPill(err);
            }
        }
    }
}
