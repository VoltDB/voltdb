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

package org.voltdb.iv2;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.WatchedEvent;
import org.apache.zookeeper_voltpatches.Watcher;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.apache.zookeeper_voltpatches.ZooDefs.Ids;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltdb.VoltDB;
import org.voltdb.VoltZK;
import org.voltdb.iv2.LeaderCache.LeaderCallBackInfo;

import com.google_voltpatches.common.base.Supplier;
import com.google_voltpatches.common.collect.ImmutableMap;
import com.google_voltpatches.common.collect.ImmutableSortedSet;

public class MpTerm implements Term
{
    VoltLogger tmLog = new VoltLogger("TM");
    private final String m_whoami;

    private final InitiatorMailbox m_mailbox;
    private final ZooKeeper m_zk;
    private volatile SortedSet<Long> m_knownLeaders = ImmutableSortedSet.of();
    private volatile Map<Integer, Long> m_cacheCopy = ImmutableMap.of();
    private boolean m_lastUpdateByMigration;
    private final ExecutorService m_es = CoreUtils.getCachedSingleThreadExecutor("mpterm", 15000);

    public static enum RepairType {
        // Repair process from partition leader changes via LeaderAppointer
        NORMAL(0),
        // Repair process from partition leader changes via migration
        MIGRATE(1),
        // Repair process not from partition leader changes
        TXN_RESTART(2),
        // Skip MP repair for leader migration while MP repair algo is cancelled.
        SKIP_MP_REPAIR(4);
        final int type;

        RepairType(int type) {
            this.type = type;
        }

        public boolean isMigrate() {
            return this == MIGRATE;
        }

        public boolean isSkipTxnRestart() {
            return this == MIGRATE || this == SKIP_MP_REPAIR;
        }

        public boolean isTxnRestart() {
            return this == TXN_RESTART;
        }

        public boolean isSkipRepair() {
            return this == SKIP_MP_REPAIR;
        }
    }

    // Initialized in start() -- when the term begins.
    protected LeaderCache m_leaderCache;
    // runs on the babysitter thread when a replica changes.
    // simply forward the notice to the initiator mailbox; it controls
    // the Term processing.
    // NOTE: The contract with LeaderCache is that it always
    // returns a full cache (one entry for every partition).  Returning a
    // partially filled cache is NotSoGood(tm).
    LeaderCache.Callback m_leadersChangeHandler = new LeaderCache.Callback()
    {
        @Override
        public void run(ImmutableMap<Integer, LeaderCallBackInfo> cache)
        {
            ImmutableSortedSet.Builder<Long> builder = ImmutableSortedSet.naturalOrder();
            ImmutableMap.Builder<Integer, Long> cacheBuilder = ImmutableMap.builder();
            boolean migratePartitionLeaderRequested = false;
            for (Entry<Integer, LeaderCallBackInfo> e : cache.entrySet()) {
                long hsid = e.getValue().m_HSId;
                builder.add(hsid);
                cacheBuilder.put(e.getKey(), hsid);
                //The master change is triggered via @MigratePartitionLeader
                if (e.getValue().m_isMigratePartitionLeaderRequested && !m_knownLeaders.contains(hsid)) {
                    migratePartitionLeaderRequested = true;
                }
            }
            final SortedSet<Long> updatedLeaders = builder.build();
            if (tmLog.isDebugEnabled()) {
                tmLog.debug(m_whoami + "LeaderCache change updating leader list to: "
                        + CoreUtils.hsIdCollectionToString(updatedLeaders) + ". MigratePartitionLeader:" + migratePartitionLeaderRequested);
            }
            m_knownLeaders = updatedLeaders;
            RepairType repairType = RepairType.NORMAL;
            if (migratePartitionLeaderRequested) {
                repairType = RepairType.MIGRATE;
            }
            m_lastUpdateByMigration = migratePartitionLeaderRequested;
            m_cacheCopy = cacheBuilder.build();
            ((MpInitiatorMailbox)m_mailbox).updateReplicas(new ArrayList<Long>(m_knownLeaders), m_cacheCopy, repairType);
        }
    };

    /**
     * Setup a new Term but don't take any action to take responsibility.
     */
    public MpTerm(ZooKeeper zk, long initiatorHSId, InitiatorMailbox mailbox,
            String whoami)
    {
        m_zk = zk;
        m_mailbox = mailbox;
        m_whoami = whoami;
    }

    /**
     * Start a new Term.  This starts watching followers via ZK.  Block on an
     * appropriate repair algorithm to watch final promotion to leader.
     */
    @Override
    public void start()
    {
        try {
            m_leaderCache = new LeaderCache(m_zk, "MpTerm-iv2masters", VoltZK.iv2masters, m_leadersChangeHandler);
            m_leaderCache.start(true);
            watchTxnRestart();
        } catch (Exception e) {
            VoltDB.crashLocalVoltDB("Unable to create babysitter starting term.", true, e);
        }
    }

    @Override
    public void shutdown()
    {
        if (m_leaderCache != null) {
            try {
                m_leaderCache.shutdown();
            } catch (InterruptedException e) {
                // We're shutting down...this may just be faster.
            }
        }
        try {
            m_es.shutdown();
            m_es.awaitTermination(5, TimeUnit.MINUTES);
        } catch (Exception e) {
        }
    }

    @Override
    public Supplier<List<Long>> getInterestingHSIds()
    {
        return new Supplier<List<Long>>() {
            @Override
            public List<Long> get() {
                return new ArrayList<Long>(m_knownLeaders);
            }
        };
    }

    private final Watcher txnRestartWatcher = new Watcher() {
        @Override
        public void process(WatchedEvent event) {
            m_es.submit(() -> {
                try {
                    watchTxnRestart();
                } catch (Exception e) {
                    VoltDB.crashLocalVoltDB(e.getMessage(), false, e);
                }}
            );
        }
    };

    // m_leadersChangeHandler and watchTxnRestart are running on the same ZK callback thread, one at a time
    // So they won't step on each other. MP transactions are repaired upon partition leader changes via m_leadersChangeHandler.
    // If no partition leader changes occur, MP transactions won't be repaired since ZK won't invoke callback m_leadersChangeHandler.
    // voltadmin stop command may hit the scenario: the host is shutdown after the last partition leader on the host is migrated away
    // If there happens to be a rerouted transaction from the last leader migration, the transaction won't be repaired via m_leadersChangeHandler.
    private void watchTxnRestart() throws KeeperException, InterruptedException {
        if(m_zk.exists(VoltZK.trigger_txn_restart, txnRestartWatcher) == null) {
            return;
        }
        if (!m_lastUpdateByMigration) {
            removeTxnRestartTrigger(m_zk);
            return;
        }
        // If any partition masters are still on dead host, let m_leadersChangeHandler process transaction repair
        Set<Integer> liveHostIds = m_mailbox.m_messenger.getLiveHostIds();
        if (!m_knownLeaders.stream().map(CoreUtils::getHostIdFromHSId).allMatch(liveHostIds::contains)) {
            removeTxnRestartTrigger(m_zk);
            return;
        }
        tmLog.info(m_whoami + "repair transaction after leader migration.");
        ((MpInitiatorMailbox)m_mailbox).updateReplicas(new ArrayList<Long>(m_knownLeaders), m_cacheCopy, RepairType.TXN_RESTART);
    }

    public static void createTxnRestartTrigger(ZooKeeper zk) {
        try {
            zk.create(VoltZK.trigger_txn_restart,
                    null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        } catch (KeeperException.NodeExistsException e) {
        } catch (Exception e) {
            VoltDB.crashLocalVoltDB("Unable to store trsansaction restart trigger info", true, e);
        }
    }

    public static void removeTxnRestartTrigger(ZooKeeper zk) {
        try {
            zk.delete(VoltZK.trigger_txn_restart, -1);
        } catch (KeeperException.NoNodeException e) {
        } catch (Exception e) {
            VoltDB.crashLocalVoltDB("Unable to delete trsansaction restart trigger info", true, e);
        }
    }
}
