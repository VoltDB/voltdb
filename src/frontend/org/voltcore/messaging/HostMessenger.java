/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

import static com.google_voltpatches.common.base.Preconditions.checkArgument;
import static com.google_voltpatches.common.base.Predicates.equalTo;
import static com.google_voltpatches.common.base.Predicates.not;

import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.ZooDefs.Ids;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltcore.agreement.AgreementSite;
import org.voltcore.agreement.InterfaceToMessenger;
import org.voltcore.common.Constants;
import org.voltcore.logging.VoltLogger;
import org.voltcore.network.PicoNetwork;
import org.voltcore.network.VoltNetworkPool;
import org.voltcore.network.VoltNetworkPool.IOStatsIntf;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.InstanceId;
import org.voltcore.utils.Pair;
import org.voltcore.utils.PortGenerator;
import org.voltcore.utils.ShutdownHooks;
import org.voltcore.zk.CoreZK;
import org.voltcore.zk.ZKUtil;
import org.voltdb.compiler.ClusterConfig.ExtensibleGroupTag;
import org.voltdb.probe.MeshProber;

import com.google_voltpatches.common.base.Preconditions;
import com.google_voltpatches.common.base.Predicate;
import com.google_voltpatches.common.collect.ImmutableList;
import com.google_voltpatches.common.collect.ImmutableMap;
import com.google_voltpatches.common.collect.ImmutableSet;
import com.google_voltpatches.common.collect.Maps;
import com.google_voltpatches.common.collect.Sets;
import com.google_voltpatches.common.net.HostAndPort;
import com.google_voltpatches.common.primitives.Longs;

/**
 * Host messenger contains all the code necessary to join a cluster mesh, and create mailboxes
 * that are addressable from anywhere within that mesh. Host messenger also provides
 * a ZooKeeper instance that is maintained within the mesh that can be used for distributed coordination
 * and failure detection.
 */
public class HostMessenger implements SocketJoiner.JoinHandler, InterfaceToMessenger {

    private static final VoltLogger m_networkLog = new VoltLogger("NETWORK");
    private static final VoltLogger m_hostLog = new VoltLogger("HOST");
    private static final VoltLogger m_tmLog = new VoltLogger("TM");

    public static final CopyOnWriteArraySet<Long> VERBOTEN_THREADS = new CopyOnWriteArraySet<Long>();

    /**
     * Callback for watching for host failures.
     */
    public interface HostWatcher {
        /**
         * Called when host failures are detected.
         * @param failedHosts    List of failed hosts, including hosts currently unknown to this host.
         */
        void hostsFailed(Set<Integer> failedHosts);
    }

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

        private static final String ACCEPTOR = "acceptor";
        private static final String NETWORK_THREADS = "networkThreads";
        private static final String BACKWARDS_TIME_FORGIVENESS_WINDOW = "backwardstimeforgivenesswindow";
        private static final String DEAD_HOST_TIMEOUT = "deadhosttimeout";
        private static final String INTERNAL_PORT = "internalport";
        private static final String INTERNAL_INTERFACE = "internalinterface";
        private static final String ZK_INTERFACE = "zkinterface";
        private static final String COORDINATOR_IP = "coordinatorip";
        private static final String RACK_AWARENESS_GROUP = "rackawarenessgroup";
        private static final String BUDDY_GROUP = "buddygroup";

        public InetSocketAddress coordinatorIp;
        public String zkInterface = "127.0.0.1:7181";
        public String internalInterface = "";
        public int internalPort = 3021;
        public String rackAwarenessgroup = "0";
        public String buddyGroup = "0";
        public int deadHostTimeout = Constants.DEFAULT_HEARTBEAT_TIMEOUT_SECONDS * 1000;
        public long backwardsTimeForgivenessWindow = 1000 * 60 * 60 * 24 * 7;
        public VoltMessageFactory factory = new VoltMessageFactory();
        public int networkThreads =  Math.max(2, CoreUtils.availableProcessors() / 4);
        public Queue<String> coreBindIds;
        public JoinAcceptor acceptor = null;

        public Config(String coordIp, int coordPort) {
            if (coordIp == null || coordIp.length() == 0) {
                coordinatorIp = new InetSocketAddress(coordPort);
            } else {
                coordinatorIp = new InetSocketAddress(coordIp, coordPort);
            }
            initNetworkThreads();
        }

        public Config() {
            this(null, org.voltcore.common.Constants.DEFAULT_INTERNAL_PORT);
            acceptor = org.voltdb.probe.MeshProber.builder()
                    .coordinators(":" + internalPort)
                    .build();
        }

        /**
         * This is for testing only. It aides test suites in the generation of
         * configurations that share the same coordinators
         * @param ports a port generator
         * @param hostCount
         * @return a list of {@link Config} that share the same coordinators
         */
        public static List<Config> generate(PortGenerator ports, int hostCount) {
            checkArgument(ports != null, "port generator is null");
            checkArgument(hostCount > 0, "host count %s is not greater than 0", hostCount);

            ImmutableList.Builder<Config> lbld = ImmutableList.builder();
            String [] coordinators = new String[hostCount];

            for (int i = 0; i < hostCount; ++i) {
                Config cnf = new Config(null, org.voltcore.common.Constants.DEFAULT_INTERNAL_PORT);
                cnf.zkInterface = "127.0.0.1:" + ports.next();
                cnf.internalPort = ports.next();
                coordinators[i] = ":" + cnf.internalPort;
                lbld.add(cnf);
            }

            List<Config> configs = lbld.build();
            MeshProber jc = org.voltdb.probe.MeshProber.builder()
                    .startAction(org.voltdb.StartAction.PROBE)
                    .hostCount(hostCount)
                    .coordinators(coordinators)
                    .build();

            for (Config cnf: configs) {
                cnf.acceptor = jc;
            }

            return configs;
        }

        public int getZKPort() {
            return HostAndPort.fromString(zkInterface)
                    .getPortOrDefault(org.voltcore.common.Constants.DEFAULT_ZK_PORT);
        }

        private void initNetworkThreads() {
            try {
                m_networkLog.info("Default network thread count: " + this.networkThreads);
                Integer networkThreadConfig = Integer.getInteger(NETWORK_THREADS);
                if ( networkThreadConfig != null ) {
                    this.networkThreads = networkThreadConfig;
                    m_networkLog.info("Overridden network thread count: " + this.networkThreads);
                }

            } catch (Exception e) {
                m_networkLog.error("Error setting network thread count", e);
            }
        }

        @Override
        public String toString() {
            JSONStringer js = new JSONStringer();
            try {
                js.object();
                js.key(RACK_AWARENESS_GROUP).value(rackAwarenessgroup);
                js.key(BUDDY_GROUP).value(buddyGroup);
                js.key(COORDINATOR_IP).value(coordinatorIp.toString());
                js.key(ZK_INTERFACE).value(zkInterface);
                js.key(INTERNAL_INTERFACE).value(internalInterface);
                js.key(INTERNAL_PORT).value(internalPort);
                js.key(DEAD_HOST_TIMEOUT).value(deadHostTimeout);
                js.key(BACKWARDS_TIME_FORGIVENESS_WINDOW).value(backwardsTimeForgivenessWindow);
                js.key(NETWORK_THREADS).value(networkThreads);
                js.key(ACCEPTOR).value(acceptor);
                js.endObject();

                return js.toString();
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Stores the information about the host's IP.
     */
    private static class HostInfo {

        private final static String HOST_IP = "hostIp";
        private final static String RACK_AWARENESS_GROUP = "rackAwarenessGroup";
        private final static String BUDDY_GROUP = "buddyGroup";

        final String m_hostIp;
        final String m_rackAwarenessGroup;
        final String m_buddyGroup;

        public HostInfo(String hostIp, String rackAwarenessGroup, String buddyGroup) {
            m_hostIp = hostIp;
            m_rackAwarenessGroup = rackAwarenessGroup;
            m_buddyGroup = buddyGroup;
        }

        public byte[] toBytes() throws JSONException
        {
            final JSONStringer js = new JSONStringer();
            js.object();
            js.key(HOST_IP).value(m_hostIp);
            js.key(RACK_AWARENESS_GROUP).value(m_rackAwarenessGroup);
            js.key(BUDDY_GROUP).value(m_buddyGroup);
            js.endObject();
            return js.toString().getBytes(StandardCharsets.UTF_8);
        }

        public static HostInfo fromBytes(byte[] bytes) throws JSONException
        {
            final JSONObject obj = new JSONObject(new String(bytes, StandardCharsets.UTF_8));
            return new HostInfo(obj.getString(HOST_IP),
                                obj.getString(RACK_AWARENESS_GROUP),
                                obj.getString(BUDDY_GROUP));
        }
    }

    // I want to make these more dynamic at some point in the future --izzy
    public static final int AGREEMENT_SITE_ID = -1;
    public static final int STATS_SITE_ID = -2;
    public static final int ASYNC_COMPILER_SITE_ID = -3;
    public static final int CLIENT_INTERFACE_SITE_ID = -4;
    public static final int SYSCATALOG_SITE_ID = -5;
    public static final int SYSINFO_SITE_ID = -6;
    public static final int SNAPSHOTSCAN_SITE_ID = -7;
    public static final int SNAPSHOTDELETE_SITE_ID = -8;
    public static final int REBALANCE_SITE_ID = -9;
    public static final int SNAPSHOT_DAEMON_ID = -10;
    public static final int SNAPSHOT_IO_AGENT_ID = -11;

    // we should never hand out this site ID.  Use it as an empty message destination
    public static final int VALHALLA = Integer.MIN_VALUE;

    int m_localHostId;

    private final Config m_config;
    private final SocketJoiner m_joiner;
    private final VoltNetworkPool m_network;
    private volatile boolean m_localhostReady = false;
    // memoized InstanceId
    private InstanceId m_instanceId = null;
    private boolean m_shuttingDown = false;
    // default to false for PD, so hopefully this gets set to true very quickly
    private AtomicBoolean m_partitionDetectionEnabled = new AtomicBoolean(false);
    private boolean m_partitionDetected = false;

    private final HostWatcher m_hostWatcher;

    private final Object m_mapLock = new Object();

    /*
     * References to other hosts in the mesh.
     * Updates via COW
     */
    volatile ImmutableMap<Integer, ForeignHost> m_foreignHosts = ImmutableMap.of();

    /*
     * References to all the local mailboxes
     * Updates via COW
     */
    volatile ImmutableMap<Long, Mailbox> m_siteMailboxes = ImmutableMap.of();

    /*
     * All failed hosts that have ever been seen.
     * Used to dedupe failures so that they are only processed once.
     */
    private volatile ImmutableSet<Integer> m_knownFailedHosts = ImmutableSet.of();

    private AgreementSite m_agreementSite;
    private ZooKeeper m_zk;
    private final AtomicInteger m_nextSiteId = new AtomicInteger(0);
    private final AtomicBoolean m_paused = new AtomicBoolean(false);

    /*
     * used when coordinating joining hosts
     */
    private final JoinAcceptor m_acceptor;

    public Mailbox getMailbox(long hsId) {
        return m_siteMailboxes.get(hsId);
    }

    /**
     * @param network
     * @param coordinatorIp
     * @param expectedHosts
     * @param catalogCRC
     * @param hostLog
     * @param membershipAcceptor
     * @param m_hostWatcher
     */
    public HostMessenger(Config config, HostWatcher hostWatcher) {
        m_config = config;
        m_hostWatcher = hostWatcher;
        m_network = new VoltNetworkPool(m_config.networkThreads, 0, m_config.coreBindIds, "Server");
        m_acceptor = config.acceptor;
        m_joiner = new SocketJoiner(
                m_config.internalInterface,
                m_config.internalPort,
                m_paused,
                m_acceptor,
                this);

        // Register a clean shutdown hook for the network threads.  This gets cranky
        // when crashLocalVoltDB() is called because System.exit() can get called from
        // a random network thread which is already shutting down and we'll get delicious
        // deadlocks.  Take the coward's way out and just don't do this if we're already
        // crashing (read as: I refuse to hunt for more shutdown deadlocks).
        ShutdownHooks.registerShutdownHook(ShutdownHooks.MIDDLE, false, new Runnable() {
            @Override
            public void run()
            {
                for (ForeignHost host : m_foreignHosts.values())
                {
                    // null is OK. It means this host never saw this host id up
                    if (host != null)
                    {
                        host.close();
                    }
                }
            }
        });

    }

    public void setPartitionDetectionEnabled(boolean enabled) {
        m_partitionDetectionEnabled.set(enabled);
    }

    /**
     * Given a set of the known host IDs before a fault, and the known host IDs in the
     * post-fault cluster, determine whether or not we think a network partition may have happened.
     * ALSO NOTE: not private so it may be unit-tested.
     */
    public static boolean makePPDDecision(int thisHostId, Set<Integer> previousHosts, Set<Integer> currentHosts, boolean pdEnabled) {

        String logLine = String.format("Partition Detection at host %d code sees current hosts [%s] and previous hosts [%s]",
                thisHostId,
                StringUtils.join(currentHosts, ','),
                StringUtils.join(previousHosts, ','));
        m_tmLog.info(logLine);

        // A strict, viable minority is always a partition.
        if ((currentHosts.size() * 2) < previousHosts.size()) {
            if (pdEnabled) {
                m_tmLog.fatal("It's possible a network partition has split the cluster into multiple viable clusters. "
                        + "Current cluster contains fewer than half of the previous servers. "
                        + "Shutting down to avoid multiple copies of the database running independently.");
                return true; // partition detection triggered
            }
            else {
                m_tmLog.warn("It's possible a network partition has split the cluster into multiple viable clusters. "
                        + "Current cluster contains fewer than half of the previous servers. "
                        + "Continuing because network partition detection is disabled, but there "
                        + "is significant danger that multiple copies of the database are running "
                        + "independently.");
                return false; // partition detection not triggered
            }
        }

        // Exact 50-50 splits. The set with the lowest survivor host doesn't trigger PPD
        // If the blessed host is in the failure set, this set is not blessed.
        if (currentHosts.size() * 2 == previousHosts.size()) {
            if (pdEnabled) {
                // find the lowest hostId between the still-alive hosts and the
                // failed hosts. Which set contains the lowest hostId?
                // This should be all the pre-partition hosts IDs.  Any new host IDs
                // (say, if this was triggered by rejoin), will be greater than any surviving
                // host ID, so don't worry about including it in this search.
                if (currentHosts.contains(Collections.min(previousHosts))) {
                    m_tmLog.info("It's possible a network partition has split the cluster into multiple viable clusters. "
                            + "Current cluster contains half of the previous servers, "
                            + "including the \"tie-breaker\" node. Continuing.");
                    return false; // partition detection not triggered
                }
                else {
                    m_tmLog.fatal("It's possible a network partition has split the cluster into multiple viable clusters. "
                            + "Current cluster contains exactly half of the previous servers, but does "
                            + "not include the \"tie-breaker\" node. "
                            + "Shutting down to avoid multiple copies of the database running independently.");
                    return true; // partition detection triggered
                }
            }
            else {
                // 50/50 split. We don't care about tie-breakers for this error message
                m_tmLog.warn("It's possible a network partition has split the cluster into multiple viable clusters. "
                        + "Current cluster contains exactly half of the previous servers. "
                        + "Continuing because network partition detection is disabled, "
                        + "but there is significant danger that multiple copies of the "
                        + "database are running independently.");
                return false; // partition detection not triggered
            }
        }

        // info message will be printed on every failure that isn't handled above (most cases)
        m_tmLog.info("It's possible a network partition has split the cluster into multiple viable clusters. "
                + "Current cluster contains a majority of the prevous servers and is safe. Continuing.");
        return false; // partition detection not triggered
    }

    private void doPartitionDetectionActivities(Set<Integer> failedHostIds)
    {
        if (m_shuttingDown) {
            return;
        }

        // We should never re-enter here once we've decided we're partitioned and doomed
        Preconditions.checkState(!m_partitionDetected, "Partition detection triggered twice.");

        // figure out previous and current cluster memberships
        Set<Integer> previousHosts = getLiveHostIds();
        Set<Integer> currentHosts = new HashSet<>(previousHosts);
        currentHosts.removeAll(failedHostIds);

        // sanity!
        Preconditions.checkState(previousHosts.contains(m_localHostId));
        Preconditions.checkState(currentHosts.contains(m_localHostId));

        // decide if we're partitioned
        // this will print out warnings if we are
        if (makePPDDecision(m_localHostId, previousHosts, currentHosts, m_partitionDetectionEnabled.get())) {
            // record here so we can ensure this only happens once for this node
            m_partitionDetected = true;
            org.voltdb.VoltDB.crashLocalVoltDB("Partition detection logic will stop this process to ensure against split brains.",
                        false, null);
        }
    }

    private final DisconnectFailedHostsCallback m_failedHostsCallback = new DisconnectFailedHostsCallback() {
        @Override
        public void disconnect(Set<Integer> failedHostIds) {
            synchronized(HostMessenger.this) {

                // Decide if the failures given could put the cluster in a split-brain
                // Then decide if we should shut down to ensure that at a MAXIMUM, only
                // one viable cluster is running.
                // This feature is called "Partition Detection" in the docs.
                doPartitionDetectionActivities(failedHostIds);
                addFailedHosts(failedHostIds);

                for (int hostId: failedHostIds) {
                    removeForeignHost(hostId);
                    if (!m_shuttingDown) {
                        // info to avoid printing on the console more than once
                        // reportForeignHostFailed should print on the console once
                        m_networkLog.info(String.format("Host %d failed (DisconnectFailedHostsCallback)", hostId));
                    }
                }
                m_acceptor.detract(failedHostIds);
                // notifying any watchers who are interested in failure -- used
                // initially to do ZK cleanup when rejoining nodes die
                if (m_hostWatcher != null) {
                    m_hostWatcher.hostsFailed(failedHostIds);
                }
            }
        }
    };

    private final void addFailedHosts(Set<Integer> rip) {
        synchronized (m_mapLock) {
            m_knownFailedHosts = ImmutableSet.<Integer>builder()
                    .addAll(Sets.filter(m_knownFailedHosts, not(in(rip))))
                    .addAll(rip)
                    .build();
        }
    }

    private final void addFailedHost(int hostId) {
        if (!m_knownFailedHosts.contains(hostId)) {
            synchronized (m_mapLock) {
                m_knownFailedHosts = ImmutableSet.<Integer>builder()
                        .addAll(Sets.filter(m_knownFailedHosts, not(equalTo(hostId))))
                        .build();
            }
        }
    }

    public synchronized void prepareForShutdown()
    {
        m_shuttingDown = true;
    }

    public synchronized boolean isShuttingDown()
    {
        return m_shuttingDown;
    }

    /**
     * Synchronization protects m_knownFailedHosts and ensures that every failed host is only reported
     * once
     */
    @Override
    public synchronized void reportForeignHostFailed(int hostId) {
        long initiatorSiteId = CoreUtils.getHSIdFromHostAndSite(hostId, AGREEMENT_SITE_ID);
        m_agreementSite.reportFault(initiatorSiteId);
        if (!m_shuttingDown) {
            // should be the single console message a user sees when another node fails
            m_networkLog.warn(String.format("Host %d failed. Cluster remains operational.", hostId));
        }
    }

    @Override
    public synchronized void relayForeignHostFailed(FaultMessage fm) {
        m_agreementSite.reportFault(fm);
        if (!m_shuttingDown) {
            m_networkLog.info("Someone else claims a host failed: " + fm);
        }
    }

    /**
     * Start the host messenger and connect to the leader, or become the leader
     * if necessary.
     *
     * @param request The requested action to send to other nodes when joining
     * the mesh. This is opaque to the HostMessenger, it can be any
     * string. HostMessenger will encode this in the request to join mesh to the
     * live hosts. The live hosts can use this request string to make further
     * decision on whether or not to accept the request.
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
                                m_config.backwardsTimeForgivenessWindow,
                                m_failedHostsCallback);
            m_agreementSite.start();
            m_agreementSite.waitForRecovery();
            m_zk = org.voltcore.zk.ZKUtil.getClient(
                    m_config.zkInterface, 60 * 1000, VERBOTEN_THREADS);
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

            /*
             * seed the leader host criteria ad leader is always host id 0
             */
            m_acceptor.accrue(selectedHostId, m_acceptor.decorate(new JSONObject(), Optional.empty()));

            // Store the components of the instance ID in ZK
            JSONObject instance_id = new JSONObject();
            instance_id.put("coord",
                    ByteBuffer.wrap(m_config.coordinatorIp.getAddress().getAddress()).getInt());
            instance_id.put("timestamp", System.currentTimeMillis());
            m_hostLog.debug("Cluster will have instance ID:\n" + instance_id.toString(4));
            byte[] payload = instance_id.toString(4).getBytes("UTF-8");
            m_zk.create(CoreZK.instance_id, payload, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

            /*
             * Store all the hosts and host ids here so that waitForGroupJoin
             * knows the size of the mesh. This part only registers this host
             */
            final HostInfo hostInfo = new HostInfo(m_config.coordinatorIp.toString(),
                    m_config.rackAwarenessgroup, m_config.buddyGroup);
            m_zk.create(CoreZK.hosts_host + selectedHostId, hostInfo.toBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        }
        zkInitBarrier.countDown();
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
                m_hostLog.error(msg);
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
    public void notifyOfJoin(
            int hostId, SocketChannel socket,
            InetSocketAddress listeningAddress,
            JSONObject jo) {
        m_networkLog.info(getHostId() + " notified of " + hostId);
        prepSocketChannel(socket);
        ForeignHost fhost = null;
        try {
            fhost = new ForeignHost(this, hostId, socket, m_config.deadHostTimeout, listeningAddress, new PicoNetwork(socket));
            putForeignHost(hostId, fhost);
            fhost.enableRead(VERBOTEN_THREADS);
        } catch (java.io.IOException e) {
            org.voltdb.VoltDB.crashLocalVoltDB("", true, e);
        }
        m_acceptor.accrue(hostId, jo);
    }

    /*
     * Set all the default options for sockets
     */
    private void prepSocketChannel(SocketChannel sc) {
        try {
            sc.socket().setSendBufferSize(1024 * 1024 * 2);
            sc.socket().setReceiveBufferSize(1024 * 1024 * 2);
        } catch (SocketException e) {
            e.printStackTrace();
        }
    }

    /*
     * Convenience method for doing the verbose COW insert into the map
     */
    private void putForeignHost(int hostId, ForeignHost fh) {
        synchronized (m_mapLock) {
            m_foreignHosts = ImmutableMap.<Integer, ForeignHost>builder()
                    .putAll(m_foreignHosts)
                    .put(hostId, fh)
                    .build();
        }
    }

    static final Predicate<Integer> in(final Set<Integer> set) {
        return new Predicate<Integer>() {
            @Override
            public boolean apply(Integer input) {
                return set.contains(input);
            }
        };
    }

    /*
     * Convenience method for doing the verbose COW remove from the map
     */
    private void removeForeignHost(int hostId) {
        ForeignHost fh = m_foreignHosts.get(hostId);
        synchronized (m_mapLock) {
            m_foreignHosts = ImmutableMap.<Integer, ForeignHost>builder()
                    .putAll(Maps.filterKeys(m_foreignHosts, not(equalTo(hostId))))
                    .build();
        }
        if (fh != null) {
            fh.close();
        }
    }

    /**
     * Any node can serve a request to join. The coordination of generating a new host id
     * is done via ZK
     *
     * @param request The requested action from the rejoining host. This is
     * opaque to the HostMessenger, it can be any string. The request string can
     * be used to make further decision on whether or not to accept the request
     * in the MembershipAcceptor.
     */
    @Override
    public void requestJoin(
            SocketChannel socket,
            InetSocketAddress listeningAddress,
            JSONObject jo) throws Exception {
        /*
         * Generate the host id via creating an ephemeral sequential node
         */
        Integer hostId = selectNewHostId(socket.socket().getInetAddress().getHostAddress());
        prepSocketChannel(socket);
        ForeignHost fhost = null;
        try {
            try {
                JoinAcceptor.PleaDecision decision = m_acceptor.considerMeshPlea(m_zk, hostId, jo);

                /*
                 * Write the response that advertises the cluster topology
                 */
                writeRequestJoinResponse(hostId, decision, socket);
                if (!decision.accepted) {
                    socket.close();
                    return;
                }

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
                        m_networkLog.info("New connection was unable to establish mesh");
                        socket.close();
                        return;
                    } else if (read < 1) {
                        Thread.sleep(5);
                    }
                }

                /*
                 * Now add the host to the mailbox system
                 */
                fhost = new ForeignHost(this, hostId, socket, m_config.deadHostTimeout, listeningAddress, new PicoNetwork(socket));
                putForeignHost(hostId, fhost);
                fhost.enableRead(VERBOTEN_THREADS);

                m_acceptor.accrue(hostId, jo);
            } catch (Exception e) {
                m_networkLog.error("Error joining new node", e);
                addFailedHost(hostId);
                removeForeignHost(hostId);
                m_acceptor.detract(hostId);
                socket.close();
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
    private void writeRequestJoinResponse(
            int hostId,
            JoinAcceptor.PleaDecision decision,
            SocketChannel socket) throws Exception {

        JSONObject jsObj = new JSONObject();

        jsObj.put("accepted", decision.accepted);
        if (decision.accepted) {
            /*
             * Tell the new node what its host id is
             */
            jsObj.put("newHostId", hostId);

            /*
             * Echo back the address that the node connected from
             */
            jsObj.put("reportedAddress",
                      ((InetSocketAddress) socket.socket().getRemoteSocketAddress()).getAddress().getHostAddress());

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
            for (Map.Entry<Integer, ForeignHost> entry : m_foreignHosts.entrySet()) {
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
        } else {
            jsObj.put("reason", decision.errMsg);
            jsObj.put("mayRetry", decision.mayRetry);
        }

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
            InetSocketAddress listeningAddresses[],
            Map<Integer, JSONObject> jos) throws Exception {
        m_localHostId = yourHostId;
        long agreementHSId = getHSIdForLocalSite(AGREEMENT_SITE_ID);

        /*
         * Construct the set of agreement sites based on all the hosts that are connected
         */
        HashSet<Long> agreementSites = new HashSet<Long>();
        agreementSites.add(agreementHSId);

        m_network.start();//network must be running for register to work

        for (int ii = 0; ii < hosts.length; ii++) {
            m_networkLog.info(yourHostId + " notified of host " + hosts[ii]);
            agreementSites.add(CoreUtils.getHSIdFromHostAndSite(hosts[ii], AGREEMENT_SITE_ID));
            prepSocketChannel(sockets[ii]);
            ForeignHost fhost = null;
            try {
                fhost = new ForeignHost(this, hosts[ii], sockets[ii], m_config.deadHostTimeout, listeningAddresses[ii], new PicoNetwork(sockets[ii]));
                putForeignHost(hosts[ii], fhost);
            } catch (java.io.IOException e) {
                org.voltdb.VoltDB.crashLocalVoltDB("Failed to instantiate foreign host", true, e);
            }
        }

        m_acceptor.accrue(jos);

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
                            m_config.backwardsTimeForgivenessWindow,
                            m_failedHostsCallback);

        /*
         * Now that the agreement site mailbox has been created it is safe
         * to enable read
         */
        for (ForeignHost fh : m_foreignHosts.values()) {
            fh.enableRead(VERBOTEN_THREADS);
        }
        m_agreementSite.start();

        /*
         * Do the usual thing of waiting for the agreement site
         * to join the cluster and creating the client
         */
        VERBOTEN_THREADS.addAll(m_network.getThreadIds());
        VERBOTEN_THREADS.addAll(m_agreementSite.getThreadIds());
        m_agreementSite.waitForRecovery();
        m_zk = org.voltcore.zk.ZKUtil.getClient(
                m_config.zkInterface, 60 * 1000, VERBOTEN_THREADS);
        if (m_zk == null) {
            throw new Exception("Timed out trying to connect local ZooKeeper instance");
        }

        /*
         * Publish the address of this node to ZK as seen by the leader
         * Also allows waitForGroupJoin to know the number of nodes in the cluster
         */
        HostInfo hostInfo;
        if (m_config.internalInterface.isEmpty()) {
            hostInfo = new HostInfo(new InetSocketAddress(m_joiner.m_reportedInternalInterface, m_config.internalPort).toString(),
                                    m_config.rackAwarenessgroup, m_config.buddyGroup);
        } else {
            hostInfo = new HostInfo(new InetSocketAddress(m_config.internalInterface, m_config.internalPort).toString(),
                                    m_config.rackAwarenessgroup, m_config.buddyGroup);
        }

        m_zk.create(CoreZK.hosts_host + getHostId(), hostInfo.toBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    }

    /**
     * Wait until all the nodes have built a mesh.
     */
    public Map<Integer, ExtensibleGroupTag> waitForGroupJoin(int expectedHosts) {
        Map<Integer, ExtensibleGroupTag> hostGroups = Maps.newHashMap();

        try {
            while (true) {
                ZKUtil.FutureWatcher fw = new ZKUtil.FutureWatcher();
                final List<String> children = m_zk.getChildren(CoreZK.hosts, fw);
                final int numChildren = children.size();

                for (String child : children) {
                    final HostInfo info = HostInfo.fromBytes(m_zk.getData(ZKUtil.joinZKPath(CoreZK.hosts, child), false, null));
                    // HostInfo ZK node name has the form "host#", hence the offset of 4 to skip the "host".
                    ExtensibleGroupTag groupTag = new ExtensibleGroupTag(info.m_rackAwarenessGroup, info.m_buddyGroup);
                    hostGroups.put(Integer.parseInt(child.substring(child.indexOf("host") + 4)), groupTag);
                }

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

        assert hostGroups.size() == expectedHosts;
        return hostGroups;
    }

    public boolean isPaused() {
        return m_paused.get();
    }

    public void unpause() {
        m_paused.set(false);
    }

    //Set Paused so socketjoiner will communicate correct status during mesh building.
    public void pause() {
        m_paused.set(true);
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

    public Set<Integer> getLiveHostIds()
    {
        Set<Integer> hostids = Sets.newTreeSet();
        hostids.addAll(m_foreignHosts.keySet());
        hostids.add(m_localHostId);
        return hostids;
    }

    /**
     * Given a hostid, return the hostname for it
     */
    @Override
    public String getHostnameForHostID(int hostId) {
        if (hostId == m_localHostId) {
            return CoreUtils.getHostnameOrAddress();
        }
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
                m_networkLog.info("Mailbox is not registered for site id " + CoreUtils.getSiteIdFromHSId(hsId));
                return null;
            }
        }

        // the foreign machine case
        ForeignHost fhost = m_foreignHosts.get(hostId);

        if (fhost == null)
        {
            if (!m_knownFailedHosts.contains(hostId)) {
                m_networkLog.warn(
                        "Attempted to send a message to foreign host with id " +
                        hostId + " but there is no such host.");
            }
            return null;
        }

        if (!fhost.isUp())
        {
            if (!m_shuttingDown) {
                m_networkLog.info("Attempted delivery of message to failed site: " + CoreUtils.hsIdToString(hsId));
            }
            return null;
        }
        return fhost;
    }

    public void registerMailbox(Mailbox mailbox) {
        if (!m_siteMailboxes.containsKey(mailbox.getHSId())) {
                throw new RuntimeException("Can only register a mailbox with an hsid alreadly generated");
        }

        synchronized (m_mapLock) {
            ImmutableMap.Builder<Long, Mailbox> b = ImmutableMap.builder();
            for (Map.Entry<Long, Mailbox> e : m_siteMailboxes.entrySet()) {
                if (e.getKey().equals(mailbox.getHSId())) {
                    b.put(e.getKey(), mailbox);
                } else {
                    b.put(e.getKey(), e.getValue());
                }
            }
            m_siteMailboxes = b.build();
        }
    }

    /*
     * Generate a slot for the mailbox and put a noop box there. Can also
     * supply a value
     */
    public long generateMailboxId(Long mailboxId) {
        final long hsId = mailboxId == null ? getHSIdForLocalSite(m_nextSiteId.getAndIncrement()) : mailboxId;
        addMailbox(hsId, new Mailbox() {
            @Override
            public void send(long hsId, VoltMessage message) {
            }

            @Override
            public void send(long[] hsIds, VoltMessage message) {
            }

            @Override
            public void deliver(VoltMessage message) {
                m_networkLog.info("No-op mailbox(" + CoreUtils.hsIdToString(hsId) + ") dropped message " + message);
            }

            @Override
            public void deliverFront(VoltMessage message) {
            }

            @Override
            public VoltMessage recv() {
                return null;
            }

            @Override
            public VoltMessage recvBlocking() {
                return null;
            }

            @Override
            public VoltMessage recvBlocking(long timeout) {
                return null;
            }

            @Override
            public VoltMessage recv(Subject[] s) {
                return null;
            }

            @Override
            public VoltMessage recvBlocking(Subject[] s) {
                return null;
            }

            @Override
            public VoltMessage recvBlocking(Subject[] s, long timeout) {
                return null;
            }

            @Override
            public long getHSId() {
                return 0L;
            }

            @Override
            public void setHSId(long hsId) {
            }

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
        addMailbox(hsId, sm);
        return sm;
    }

    private void addMailbox(long hsId, Mailbox m) {
        synchronized (m_mapLock) {
            ImmutableMap.Builder<Long, Mailbox> b = ImmutableMap.builder();
            b.putAll(m_siteMailboxes);
            b.put(hsId, m);
            m_siteMailboxes = b.build();
        }
    }

    /**
     * Discard a mailbox
     */
    public void removeMailbox(long hsId) {
        synchronized (m_mapLock) {
            ImmutableMap.Builder<Long, Mailbox> b = ImmutableMap.builder();
            for (Map.Entry<Long, Mailbox> e : m_siteMailboxes.entrySet()) {
                if (e.getKey().equals(hsId)) continue;
                b.put(e.getKey(), e.getValue());
            }
            m_siteMailboxes = b.build();
        }
    }

    public void send(final long destinationHSId, final VoltMessage message)
    {
        assert(message != null);

        ForeignHost host = presend(destinationHSId, message);
        if (host != null) {
            host.send(new long [] { destinationHSId }, message);
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
            e.getKey().send(Longs.toArray(e.getValue()), message);
        }
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
        if (m_zk != null) {
            m_zk.close();
        }
        if (m_agreementSite != null) {
            m_agreementSite.shutdown();
        }
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
        VERBOTEN_THREADS.clear();
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

        addMailbox(hsId, mailbox);
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

    public JoinAcceptor getAcceptor() {
        return m_acceptor;
    }

    public void sendPoisonPill(Collection<Integer> hostIds, String err, int cause) {
        for (int hostId : hostIds) {
            ForeignHost fh = m_foreignHosts.get(hostId);
            if (fh != null && fh.isUp()) {
                fh.sendPoisonPill(err, ForeignHost.CRASH_SPECIFIED);
            }
        }
    }

    public void sendPoisonPill(String err) {
        for (int hostId : m_foreignHosts.keySet()) {
            ForeignHost fh = m_foreignHosts.get(hostId);
            if (fh != null && fh.isUp()) {
                fh.sendPoisonPill(err, ForeignHost.CRASH_ALL);
            }
        }
    }

    public void sendPoisonPill(String err, int targetHostId, int cause) {
        ForeignHost fh = m_foreignHosts.get(targetHostId);
        if (fh != null && fh.isUp()) {
            fh.sendPoisonPill(err, cause);
        }
    }

    public boolean validateForeignHostId(Integer hostId) {
        return !m_knownFailedHosts.contains(hostId);
    }

    public void setDeadHostTimeout(int timeout) {
        Preconditions.checkArgument(timeout > 0, "Timeout value must be > 0, was %s", timeout);
        m_hostLog.info("Dead host timeout set to " + timeout + " milliseconds");
        m_config.deadHostTimeout = timeout;
        for (ForeignHost fh : m_foreignHosts.values()) {
            fh.updateDeadHostTimeout(timeout);
        }
    }

    public Map<Long, Pair<String, long[]>>
        getIOStats(final boolean interval) throws InterruptedException, ExecutionException {
        final ImmutableMap<Integer, ForeignHost> fhosts = m_foreignHosts;
        ArrayList<IOStatsIntf> picoNetworks = new ArrayList<IOStatsIntf>(fhosts.size());

        for (ForeignHost fh : fhosts.values()) {
            picoNetworks.add(fh.m_network);
        }

        return m_network.getIOStats(interval, picoNetworks);
    }

    /**
     * Cut the network connection between two hostids immediately
     * Useful for simulating network partitions
     */
    public void cutLink(int hostIdA, int hostIdB) {
        if (m_localHostId == hostIdA) {
            ForeignHost fh = m_foreignHosts.get(hostIdB);
            if (fh != null) {
                fh.cutLink();
            }
        }
        if (m_localHostId == hostIdB) {
            ForeignHost fh = m_foreignHosts.get(hostIdA);
            if (fh != null) {
                fh.cutLink();
            }
        }
    }

}
