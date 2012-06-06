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
import java.util.Comparator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeSet;

import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.ZooKeeper;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;

import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.MessagingException;
import org.voltcore.messaging.VoltMessage;
import org.voltcore.zk.BabySitter;
import org.voltcore.zk.BabySitter.Callback;
import org.voltcore.zk.LeaderElector;
import org.voltcore.zk.MapCache;
import org.voltcore.zk.MapCacheWriter;

import org.voltdb.messaging.Iv2RepairLogRequestMessage;
import org.voltdb.messaging.Iv2RepairLogResponseMessage;
import org.voltdb.VoltDB;
import org.voltdb.VoltZK;

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
public class Term
{
    VoltLogger hostLog = new VoltLogger("HOST");

    private final InitiatorMailbox m_mailbox;
    private final int m_partitionId;
    private final long m_initiatorHSId;
    private final int m_requestId = 0; // System.currentTimeMillis();
    private final ZooKeeper m_zk;

    // Initialized in start() -- when the term begins.
    private BabySitter m_babySitter;

    // scoreboard for responding replica repair log responses (hsid -> response count)
    private static class ReplicaRepairStruct {
        int m_receivedResponses = 0;
        int m_expectedResponses = -1;
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
    Comparator<Iv2RepairLogResponseMessage> m_unionComparator = new Comparator<Iv2RepairLogResponseMessage>() {
        @Override
        public int compare(Iv2RepairLogResponseMessage o1, Iv2RepairLogResponseMessage o2)
        {
            return (int)(o1.getSpHandle() - o2.getSpHandle());
        }
    };

    // Union of repair responses.
    TreeSet<Iv2RepairLogResponseMessage> m_repairLogUnion =
        new TreeSet<Iv2RepairLogResponseMessage>();

    Callback m_replicasChangeHandler = new Callback()
    {
        @Override
        public void run(List<String> children) {
            hostLog.info("Babysitter for zkLeaderNode: " +
                    LeaderElector.electionDirForPartition(m_partitionId) + ":");
            hostLog.info("children: " + children);
            // make an HSId array out of the children
            // The list contains the leader, skip it
            List<Long> replicas = childrenToReplicaHSIds(m_initiatorHSId, children);
            hostLog.info("Updated replicas: " + replicas);
            m_mailbox.updateReplicas(replicas);
        }
    };

    // conversion helper.
    static List<Long> childrenToReplicaHSIds(long initiatorHSId, List<String> children)
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

    // future that represents completion of transition to leader.
    public static class InaugurationFuture implements Future<Boolean>
    {
        private CountDownLatch m_doneLatch = new CountDownLatch(1);
        private ExecutionException m_exception = null;

        private void setException(Exception e)
        {
            m_exception = new ExecutionException(e);
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return false;
        }

        @Override
        public Boolean get() throws InterruptedException, ExecutionException {
            m_doneLatch.await();
            if (m_exception != null) {
                throw m_exception;
            }
            return true;
        }

        @Override
        public Boolean get(long timeout, TimeUnit unit)
                throws InterruptedException, ExecutionException,
                TimeoutException {
            m_doneLatch.await(timeout, unit);
            if (m_exception != null) {
                throw m_exception;
            }
            return true;
        }
    }

    /**
     * Setup a new Term but don't take any action to take responsibility.
     */
    public Term(ZooKeeper zk, int partitionId, long initiatorHSId, InitiatorMailbox mailbox)
        throws ExecutionException, InterruptedException, KeeperException
    {
        m_zk = zk;
        m_partitionId = partitionId;
        m_initiatorHSId = initiatorHSId;
        m_mailbox = mailbox;
    }

    /**
     * Start a new Term. Returns a future that is done when the leadership has
     * been fully assumed and all surviving replicas have been repaired.
     *
     * @param kfactorForStartup If running for startup and not for fault
     * recovery, pass the kfactor required to proceed. For fault recovery,
     * pass any negative value as kfactorForStartup.
     */
    public Future<?> start(int kfactorForStartup)
    {
        InaugurationFuture result = new InaugurationFuture();
        try {
            m_babySitter = new BabySitter(m_zk,
                    LeaderElector.electionDirForPartition(m_partitionId),
                    m_replicasChangeHandler, true);
            if (kfactorForStartup >= 0) {
                prepareForStartup(kfactorForStartup);
                result.m_doneLatch.countDown();
            }
            else {
                prepareForFaultRecovery();
            }
        } catch (Exception e) {
            result.setException(e);
            result.m_doneLatch.countDown();
        }
        return result;
    }


    public Future<?> cancel()
    {
        // TODO: make this do something
        return null;
    }

    public void shutdown()
    {
        if (m_babySitter != null) {
            m_babySitter.shutdown();
        }
    }


    /** Block until all replica's are present. */
    void prepareForStartup(int kfactor)
    {
        List<String> children = m_babySitter.lastSeenChildren();
        while (children.size() < kfactor + 1) {
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
            }
            children = m_babySitter.lastSeenChildren();
        }
        declareReadyAsLeader();
    }

    /** Start fixing replicas: setup scoreboard and request repair logs. */
    void prepareForFaultRecovery()
    {
        List<String> survivorsNames = m_babySitter.lastSeenChildren();
        List<Long> survivors =  childrenToReplicaHSIds(m_initiatorHSId, survivorsNames);

        for (Long hsid : survivors) {
            m_replicaRepairStructs.put(hsid, new ReplicaRepairStruct());
        }

        VoltMessage logRequest = new Iv2RepairLogRequestMessage(m_requestId);
        try {
            m_mailbox.send(com.google.common.primitives.Longs.toArray(survivors), logRequest);
        }
        catch (MessagingException ignored) {
        }
    }

    /** Process a new repair log response */
    public void deliver(VoltMessage message)
    {
        if (message instanceof Iv2RepairLogResponseMessage) {
            Iv2RepairLogResponseMessage response = (Iv2RepairLogResponseMessage)message;
            if (response.getRequestId() != m_requestId) {
                return;
            }
            ReplicaRepairStruct rrs = m_replicaRepairStructs.get(response.m_sourceHSId);
            m_repairLogUnion.add(response);
            int waitingFor = rrs.update(response);
            // if a replica finished, see if they're all done...
            if (waitingFor == 0 && repairLogsAreComplete()) {
                repairSurvivors();
            }
        }
    }

    /** Have all survivors supplied a full repair log? */
    public boolean repairLogsAreComplete()
    {
        for (Entry<Long, ReplicaRepairStruct> entry : m_replicaRepairStructs.entrySet()) {
            if (entry.getValue().logsComplete() != 0) {
                return false;
            }
        }
        return true;
    }

    /** Send missed-messages to survivors. Exciting! */
    public void repairSurvivors()
    {
        for (Iv2RepairLogResponseMessage li : m_repairLogUnion) {
            for (Entry<Long, ReplicaRepairStruct> entry : m_replicaRepairStructs.entrySet()) {
                if  (entry.getValue().needs(li.getSpHandle())) {
                    m_mailbox.repairReplicaWith(entry.getKey(), li);
                }
            }
        }
    }


    // with leadership election complete, update the master list
    // for non-initiator components that care.
    void declareReadyAsLeader()
    {
        hostLog.info("Registering " +  m_partitionId + " as new master.");
        try {
            MapCacheWriter iv2masters = new MapCache(m_zk, VoltZK.iv2masters);
            iv2masters.put(Integer.toString(m_partitionId),
                    new JSONObject("{hsid:" + m_mailbox.getHSId() + "}"));
        } catch (KeeperException e) {
            VoltDB.crashLocalVoltDB("Bad news: failed to declare leader.", true, e);
        } catch (InterruptedException e) {
            VoltDB.crashLocalVoltDB("Bad news: failed to declare leader.", true, e);
        } catch (JSONException e) {
            VoltDB.crashLocalVoltDB("Bad news: failed to declare leader.", true, e);
        }
    }


}
