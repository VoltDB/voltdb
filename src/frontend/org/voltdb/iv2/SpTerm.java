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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.Pair;
import org.voltcore.zk.BabySitter;
import org.voltcore.zk.BabySitter.Callback;
import org.voltcore.zk.LeaderElector;
import org.voltdb.VoltDB;
import org.voltdb.VoltZK;
import org.voltdb.dtxn.TransactionState;

import com.google_voltpatches.common.base.Supplier;
import com.google_voltpatches.common.collect.ImmutableList;
import com.google_voltpatches.common.collect.Sets;

public class SpTerm implements Term
{
    VoltLogger tmLog = new VoltLogger("TM");
    private final String m_whoami;

    private final InitiatorMailbox m_mailbox;
    private final int m_partitionId;
    private final ZooKeeper m_zk;

    // Initialized in start() -- when the term begins.
    protected BabySitter m_babySitter;
    private ImmutableList<Long> m_replicas = ImmutableList.of();
    private boolean m_replicasUpdatedRequired = false;
    private boolean m_initJoin = VoltDB.instance().isJoining();
    private final int m_kFactor = VoltDB.instance().getKFactor();
    private boolean m_promoting = false;

    // runs on the babysitter thread when a replica changes.
    // simply forward the notice to the initiator mailbox; it controls
    // the Term processing.
    Callback m_replicasChangeHandler = new Callback()
    {
        @Override
        public void run(List<String> children)
        {
            // remove the leader; convert to hsids; deal with the replica change.
            List<Long> replicas = VoltZK.childrenToReplicaHSIds(children);
            if (tmLog.isDebugEnabled()) {
                tmLog.debug(m_whoami
                      + "replica change handler updating replica list to: "
                      + CoreUtils.hsIdCollectionToString(replicas) +
                      " from " +
                      CoreUtils.hsIdCollectionToString(m_replicas));
            }
            if (replicas.size() == m_replicas.size()) {
                Set<Long> diff = Sets.difference(new HashSet<Long>(replicas),
                                                 new HashSet<Long>(m_replicas));
                if (diff.isEmpty()) {
                    return;
                }
            }

            // Since we don't shutdown SpTerm when current site is no longer leader
            // (see explanation at SpInitiator, m_leadersChangeHandler handler),
            // ask non-leader (from scheduler perspective) to ignore replica list change.
            if (!m_promoting && !m_mailbox.m_scheduler.isLeader()) {
                if (replicas.size() != m_replicas.size()) {
                   m_mailbox.updateReplicas(replicas, null);
                }
                m_replicas = ImmutableList.copyOf(replicas);
                return;
            }

            // for joining nodes that hasn't been fully initialized
            // still update replicas for allowing all replicas receive fragment tasks
            if (m_initJoin) {
                if (replicas.size() == m_kFactor + 1) {
                    m_initJoin = false;
                }
                m_mailbox.updateReplicas(replicas, null);
                m_replicasUpdatedRequired = false;
            }
            if (m_replicas.isEmpty() || replicas.size() <= m_replicas.size()) {
                //The cases for startup or host failure
                m_mailbox.updateReplicas(replicas, null);
                m_replicasUpdatedRequired = false;
            } else {
                //The case for rejoin
                m_replicasUpdatedRequired = true;
                tmLog.info(m_whoami + " replicas to be updated from join:"
                          + CoreUtils.hsIdCollectionToString(m_replicas));
            }
            m_replicas = ImmutableList.copyOf(replicas);
        }
    };

    /**
     * Setup a new Term but don't take any action to take responsibility.
     */
    public SpTerm(ZooKeeper zk, int partitionId, long initiatorHSId, InitiatorMailbox mailbox,
            String whoami)
    {
        m_zk = zk;
        m_partitionId = partitionId;
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
            m_promoting = true;
            Pair<BabySitter, List<String>> pair = BabySitter.blockingFactory(m_zk,
                    LeaderElector.electionDirForPartition(VoltZK.leaders_initiators, m_partitionId),
                    m_replicasChangeHandler);
            m_babySitter = pair.getFirst();
        }
        catch (ExecutionException ee) {
            VoltDB.crashLocalVoltDB("Unable to create babysitter starting term.", true, ee);
        } catch (InterruptedException e) {
            VoltDB.crashLocalVoltDB("Unable to create babysitter starting term.", true, e);
        }
    }

    @Override
    public void shutdown()
    {
        if (m_babySitter != null) {
            m_babySitter.shutdown();
        }
        m_replicas = ImmutableList.of();
    }

    @Override
    public Supplier<List<Long>> getInterestingHSIds()
    {
        return new Supplier<List<Long>>() {
            @Override
            public List<Long> get() {
                List<String> survivorsNames = m_babySitter.lastSeenChildren();
                List<Long> survivors =  VoltZK.childrenToReplicaHSIds(survivorsNames);
                return survivors;
            }
        };
    }

    //replica update is delayed till this is called during joining or rejoing snapshot
    // mpTxnId: the MP transaction id of ongoing stream snapshot save
    public long[] updateReplicas(TransactionState snapshotTransactionState) {
        long[] replicasAdded = new long[0];
        if (m_replicasUpdatedRequired) {
            tmLog.info(m_whoami + " updated replica list to: "
                    + CoreUtils.hsIdCollectionToString(m_replicas));
            replicasAdded = m_mailbox.updateReplicas(m_replicas, null, snapshotTransactionState);
            m_replicasUpdatedRequired = false;
        }
        return replicasAdded;
    }

    public void setPromoting(boolean promoting) {
        m_promoting = promoting;
    }
}
