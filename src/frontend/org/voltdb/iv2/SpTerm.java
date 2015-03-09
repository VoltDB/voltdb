/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

import java.util.concurrent.ExecutionException;
import java.util.List;

import com.google_voltpatches.common.base.Supplier;
import org.apache.zookeeper_voltpatches.ZooKeeper;

import org.voltcore.logging.VoltLogger;

import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.Pair;
import org.voltcore.zk.BabySitter;
import org.voltcore.zk.BabySitter.Callback;
import org.voltcore.zk.LeaderElector;

import org.voltdb.VoltDB;
import org.voltdb.VoltZK;

public class SpTerm implements Term
{
    VoltLogger tmLog = new VoltLogger("TM");
    private final String m_whoami;

    private final InitiatorMailbox m_mailbox;
    private final int m_partitionId;
    private final ZooKeeper m_zk;

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
            // remove the leader; convert to hsids; deal with the replica change.
            List<Long> replicas = VoltZK.childrenToReplicaHSIds(children);
            tmLog.debug(m_whoami
                      + "replica change handler updating replica list to: "
                      + CoreUtils.hsIdCollectionToString(replicas));

            m_mailbox.updateReplicas(replicas, null);
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
}
