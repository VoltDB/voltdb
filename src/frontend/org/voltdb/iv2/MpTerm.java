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

package org.voltdb.iv2;

import java.lang.InterruptedException;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.List;
import java.util.TreeSet;

import org.apache.zookeeper_voltpatches.ZooKeeper;

import org.voltcore.logging.VoltLogger;

import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.Pair;
import org.voltcore.zk.BabySitter;
import org.voltcore.zk.BabySitter.Callback;
import org.voltcore.zk.LeaderElector;

import org.voltdb.VoltDB;

import com.google.common.collect.Sets;

public class MpTerm implements Term
{
    VoltLogger tmLog = new VoltLogger("TM");
    private final String m_whoami;

    private final InitiatorMailbox m_mailbox;
    private final int m_partitionId;
    private final ZooKeeper m_zk;
    private final CountDownLatch m_missingStartupSites;
    private final TreeSet<String> m_knownReplicas = new TreeSet<String>();

    // Initialized in start() -- when the term begins.
    protected BabySitter m_babySitter;

    // runs on the babysitter thread when a replica changes.
    // simply forward the notice to the initiator mailbox; it controls
    // the Term processing.
    Callback m_replicasChangeHandler = new Callback()
    {
        @Override
        public void run(List<String> children)
        {
            // Need to handle startup separately from runtime updates.
            if (MpTerm.this.m_missingStartupSites.getCount() > 0) {
                TreeSet<String> updatedReplicas = com.google.common.collect.Sets.newTreeSet(children);
                // Cancel setup if a previously seen replica disappeared.
                // I think voltcore might actually terminate before getting here...
                Sets.SetView<String> removed = Sets.difference(MpTerm.this.m_knownReplicas, updatedReplicas);
                if (!removed.isEmpty()) {
                    tmLog.error(m_whoami
                            + "replica(s) failed during startup. Initialization can not complete."
                            + " Failed replicas: " + removed);
                    VoltDB.crashLocalVoltDB("Replicas failed during startup.", true, null);
                    return;
                }
                Sets.SetView<String> added = Sets.difference(updatedReplicas, MpTerm.this.m_knownReplicas);
                int newReplicas = added.size();
                m_knownReplicas.addAll(updatedReplicas);
                List<Long> replicas = BaseInitiator.childrenToReplicaHSIds(updatedReplicas);
                m_mailbox.updateReplicas(replicas);
                for (int i=0; i < newReplicas; i++) {
                    MpTerm.this.m_missingStartupSites.countDown();
                }
            }
            else {
                // remove the leader; convert to hsids; deal with the replica change.
                List<Long> replicas = BaseInitiator.childrenToReplicaHSIds(children);
                tmLog.info(m_whoami
                        + "replica change handler updating replica list to: "
                        + CoreUtils.hsIdCollectionToString(replicas));
                m_mailbox.updateReplicas(replicas);
            }
        }
    };

    /**
     * Setup a new Term but don't take any action to take responsibility.
     */
    public MpTerm(CountDownLatch missingStartupSites, ZooKeeper zk,
            int partitionId, long initiatorHSId, InitiatorMailbox mailbox,
            String zkMapCacheNode, String whoami)
    {
        m_zk = zk;
        m_partitionId = partitionId;
        m_mailbox = mailbox;

        if (missingStartupSites != null) {
            m_missingStartupSites = missingStartupSites;
        }
        else {
            m_missingStartupSites = new CountDownLatch(0);
        }

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
            Pair<BabySitter, List<String>> pair = BabySitter.blockingFactory(m_zk,
                    LeaderElector.electionDirForPartition(m_partitionId),
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
    }

    @Override
    public List<Long> getInterestingHSIds()
    {
        List<String> survivorsNames = m_babySitter.lastSeenChildren();
        List<Long> survivors =  BaseInitiator.childrenToReplicaHSIds(survivorsNames);
        return survivors;
    }
}
