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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.ZooKeeper;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;

import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.VoltMessage;

import org.voltcore.zk.LeaderElector;
import org.voltcore.zk.MapCache;
import org.voltcore.zk.MapCacheWriter;

import org.voltdb.messaging.Iv2RepairLogResponseMessage;
import org.voltdb.VoltDB;

// Some comments on threading and organization.
//   start() returns a future. Block on this future to get the final answer.
//
//   deliver() runs in the initiator mailbox deliver() context and triggers
//   all repair work.
//
//   replica change handler runs in the babysitter thread context.
//   replica change handler invokes a method in init.mbox that also
//   takes the init.mbox deliver lock
//
//   it is important that repair work happens with the deliver lock held
//   and that updatereplicas also holds this lock -- replica failure during
//   repair must happen unambigously before or after each local repair action.
//
//   A Term can be cancelled by initiator mailbox while the deliver lock is
//   held. Repair work must check for cancellation before producing repair
//   actions to the mailbox.
//
//   Note that a term can not prevent messages being delivered post cancellation.
//   RepairLog requests therefore use a requestId to dis-ambiguate responses
//   for cancelled requests that are filtering in late.


/**
 * Term encapsulates the process/algorithm of becoming
 * a new PI and the consequent ZK observers for performing that
 * role.
 */
public class StartupAlgo implements RepairAlgo
{
    VoltLogger tmLog = new VoltLogger("TM");
    private final String m_whoami;

    private final InitiatorMailbox m_mailbox;
    private final int m_partitionId;
    private final long m_requestId = System.nanoTime();
    private final ZooKeeper m_zk;
    private final CountDownLatch m_missingStartupSites;
    private final TreeSet<String> m_knownReplicas = new TreeSet<String>();
    private final String m_mapCacheNode;

    // Each Term can process at most one promotion; if promotion fails, make
    // a new Term and try again (if that's your big plan...)
    private final InaugurationFuture m_promotionResult = new InaugurationFuture();

    long getRequestId()
    {
        return m_requestId;
    }

    // scoreboard for responding replica repair log responses (hsid -> response count)
    static class ReplicaRepairStruct
    {
        int m_receivedResponses = 0;
        int m_expectedResponses = -1; // (a log msg cares about this init. value)
        long m_maxSpHandleSeen = Long.MIN_VALUE;

        // update counters and return the number of outstanding messages.
        int update(Iv2RepairLogResponseMessage response)
        {
            m_receivedResponses++;
            m_expectedResponses = response.getOfTotal();
            m_maxSpHandleSeen = Math.max(m_maxSpHandleSeen, response.getSpHandle());
            return logsComplete();
        }

        // return 0 if all expected logs have been received.
        int logsComplete()
        {
            // expected responses is really a count of remote
            // messages. if there aren't any, the sequence will be
            // 1 (the count of responses) while expected will be 0
            // (the length of the remote log)
            if (m_expectedResponses == 0) {
               return 0;
            }
            return m_expectedResponses - m_receivedResponses;
        }

        // return true if this replica needs the message for spHandle.
        boolean needs(long spHandle)
        {
            return m_maxSpHandleSeen < spHandle;
        }
    }

    // replicas being processed and repaired.
    Map<Long, ReplicaRepairStruct> m_replicaRepairStructs =
        new HashMap<Long, ReplicaRepairStruct>();

    // Determine equal repair responses by the SpHandle of the response.
    Comparator<Iv2RepairLogResponseMessage> m_unionComparator =
        new Comparator<Iv2RepairLogResponseMessage>()
    {
        @Override
        public int compare(Iv2RepairLogResponseMessage o1, Iv2RepairLogResponseMessage o2)
        {
            return (int)(o1.getSpHandle() - o2.getSpHandle());
        }
    };

    // Union of repair responses.
    TreeSet<Iv2RepairLogResponseMessage> m_repairLogUnion =
        new TreeSet<Iv2RepairLogResponseMessage>(m_unionComparator);


    // conversion helper.
    static List<Long> childrenToReplicaHSIds(long initiatorHSId, Collection<String> children)
    {
        List<Long> replicas = new ArrayList<Long>(children.size() - 1);
        for (String child : children) {
            long HSId = Long.parseLong(LeaderElector.getPrefixFromChildName(child));
            if (HSId != initiatorHSId)
            {
                replicas.add(HSId);
            }
        }
        return replicas;
    }


    /**
     * Setup a new Term but don't take any action to take responsibility.
     */
    public StartupAlgo(CountDownLatch missingStartupSites, ZooKeeper zk,
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
        m_mapCacheNode = zkMapCacheNode;
    }

    /**
     * Start a new Term. Returns a future that is done when the leadership has
     * been fully assumed and all surviving replicas have been repaired.
     *
     * @param kfactorForStartup If running for startup and not for fault
     * recovery, pass the kfactor required to proceed. For fault recovery,
     * pass any negative value as kfactorForStartup.
     */
    public Future<Boolean> start(List<Long> survivors)
    {
        try {
            prepareForStartup();
        } catch (Exception e) {
            tmLog.error(m_whoami + "failed leader promotion:", e);
            m_promotionResult.setException(e);
            m_promotionResult.done();
        }
        return m_promotionResult;
    }

    public boolean cancel()
    {
        return false;
    }

    public void shutdown()
    {
    }

    /** Block until all replica's are present. */
    void prepareForStartup()
        throws InterruptedException
    {
        tmLog.info(m_whoami +
                "starting leader promotion with " + m_knownReplicas.size() + " replicas. " +
                "Waiting for " + m_missingStartupSites.getCount() + " more for configured k-safety.");

        // block here until the babysitter thread provides all replicas.
        // then initialize the mailbox's replica set and proceed as leader.
        m_missingStartupSites.await();
        declareReadyAsLeader();
    }

    /** Process a new repair log response */
    public void deliver(VoltMessage message)
    {
        if (message instanceof Iv2RepairLogResponseMessage) {
            throw new RuntimeException("Dude, shouldn't get these here.");
        }
    }

    // with leadership election complete, update the master list
    // for non-initiator components that care.
    void declareReadyAsLeader()
    {
        try {
            MapCacheWriter iv2masters = new MapCache(m_zk, m_mapCacheNode);
            iv2masters.put(Integer.toString(m_partitionId),
                    new JSONObject("{hsid:" + m_mailbox.getHSId() + "}"));
            m_promotionResult.done();
        } catch (KeeperException e) {
            VoltDB.crashLocalVoltDB("Bad news: failed to declare leader.", true, e);
        } catch (InterruptedException e) {
            VoltDB.crashLocalVoltDB("Bad news: failed to declare leader.", true, e);
        } catch (JSONException e) {
            VoltDB.crashLocalVoltDB("Bad news: failed to declare leader.", true, e);
        }
    }
}
