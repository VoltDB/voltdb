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

import java.util.concurrent.atomic.AtomicBoolean;
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
import org.voltcore.messaging.VoltMessage;

import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.Pair;
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
    VoltLogger tmLog = new VoltLogger("TM");

    private final InitiatorMailbox m_mailbox;
    private final int m_partitionId;
    private final long m_initiatorHSId;
    private final long m_requestId = System.nanoTime();
    private final ZooKeeper m_zk;
    private final String m_whoami;

    // Initialized in start() -- when the term begins.
    protected BabySitter m_babySitter;

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

    // runs on the babysitter thread when a replica changes.
    // simply forward the notice to the initiator mailbox; it controls
    // the Term processing.
    Callback m_replicasChangeHandler = new Callback()
    {
        @Override
        public void run(List<String> children) {
            // make an HSId array out of the children
            // The list contains the leader, skip it
            List<Long> replicas = childrenToReplicaHSIds(m_initiatorHSId, children);
            tmLog.info(m_whoami
                    + "replica change handler updating replica list to: "
                    + CoreUtils.hsIdCollectionToString(replicas));
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
    private static class InaugurationFuture implements Future<Boolean>
    {
        private CountDownLatch m_doneLatch = new CountDownLatch(1);
        private AtomicBoolean m_cancelled = new AtomicBoolean(false);
        private ExecutionException m_exception = null;

        private void setException(Exception e)
        {
            m_exception = new ExecutionException(e);
        }

        private Boolean result() throws ExecutionException
        {
            if (m_exception != null) {
                throw m_exception;
            }
            else if (isCancelled()) {
                return false;
            }
            return true;

        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning)
        {
            if (isDone()) {
                return false;
            }
            else {
                m_cancelled.set(true);
                m_doneLatch.countDown();
                return true;
            }
        }

        @Override
        public boolean isCancelled()
        {
            return m_cancelled.get();
        }

        @Override
        public boolean isDone()
        {
            return m_doneLatch.getCount() == 0;
        }

        @Override
        public Boolean get() throws InterruptedException, ExecutionException
        {
            m_doneLatch.await();
            return result();
        }

        @Override
        public Boolean get(long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException
        {
            m_doneLatch.await(timeout, unit);
            return result();
        }
    }

    // Each Term can process at most one promotion; if promotion fails, make
    // a new Term and try again (if that's your big plan...)
    private final InaugurationFuture m_promotionResult = new InaugurationFuture();

    /**
     * Setup a new Term but don't take any action to take responsibility.
     */
    public Term(ZooKeeper zk, int partitionId, long initiatorHSId, InitiatorMailbox mailbox)
    {
        m_zk = zk;
        m_partitionId = partitionId;
        m_initiatorHSId = initiatorHSId;
        m_mailbox = mailbox;
        m_whoami = "SP " +  CoreUtils.hsIdToString(m_initiatorHSId)
            + " for partition " + m_partitionId + " ";
    }

    /**
     * Start a new Term. Returns a future that is done when the leadership has
     * been fully assumed and all surviving replicas have been repaired.
     *
     * @param kfactorForStartup If running for startup and not for fault
     * recovery, pass the kfactor required to proceed. For fault recovery,
     * pass any negative value as kfactorForStartup.
     */
    public Future<Boolean> start(int kfactorForStartup)
    {
        try {
            makeBabySitter();
            if (kfactorForStartup >= 0) {
                prepareForStartup(kfactorForStartup);
            }
            else {
                prepareForFaultRecovery();
            }
        } catch (Exception e) {
            tmLog.error(m_whoami + "failed leader promotion:", e);
            m_promotionResult.setException(e);
            m_promotionResult.m_doneLatch.countDown();
        }
        return m_promotionResult;
    }

    // extract this out so it can be mocked for testcases.
    // don't want to dep-inject this because the whole point is
    // that term encapsulates the babysitter...
    protected void makeBabySitter() throws ExecutionException, InterruptedException
    {
        Pair<BabySitter, List<String>> pair = BabySitter.blockingFactory(m_zk,
                LeaderElector.electionDirForPartition(m_partitionId),
                m_replicasChangeHandler);
        m_babySitter = pair.getFirst();
    }

    public boolean cancel()
    {
        return m_promotionResult.cancel(false);
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
        tmLog.info(m_whoami + "starting with " + kfactor + " replicas.");
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

    /** Start fixing survivors: setup scoreboard and request repair logs. */
    void prepareForFaultRecovery()
    {
        List<String> survivorsNames = m_babySitter.lastSeenChildren();
        List<Long> survivors =  childrenToReplicaHSIds(m_initiatorHSId, survivorsNames);
        survivors.add(m_initiatorHSId);

        for (Long hsid : survivors) {
            m_replicaRepairStructs.put(hsid, new ReplicaRepairStruct());
        }

        tmLog.info(m_whoami + "found (including self) " + survivors.size()
                + " surviving replicas to repair. "
                + " Survivors: " + CoreUtils.hsIdCollectionToString(survivors));
        VoltMessage logRequest = new Iv2RepairLogRequestMessage(m_requestId);
        m_mailbox.send(com.google.common.primitives.Longs.toArray(survivors), logRequest);
    }

    /** Process a new repair log response */
    public void deliver(VoltMessage message)
    {
        if (message instanceof Iv2RepairLogResponseMessage) {
            Iv2RepairLogResponseMessage response = (Iv2RepairLogResponseMessage)message;
            if (response.getRequestId() != m_requestId) {
                tmLog.info(m_whoami + "rejecting stale repair response."
                        + " Current request id is: " + m_requestId
                        + " Received response for request id: " + response.getRequestId());
                return;
            }
            ReplicaRepairStruct rrs = m_replicaRepairStructs.get(response.m_sourceHSId);
            if (rrs.m_expectedResponses < 0) {
                tmLog.info(m_whoami + "collecting " + response.getOfTotal()
                        + " repair log entries from "
                        + CoreUtils.hsIdToString(response.m_sourceHSId));
            }
            m_repairLogUnion.add(response);
            if (rrs.update(response) == 0) {
                tmLog.info(m_whoami + "collected " + rrs.m_receivedResponses
                        + " responses for " + rrs.m_expectedResponses +
                        " repair log entries from " + CoreUtils.hsIdToString(response.m_sourceHSId));
                if (areRepairLogsComplete()) {
                    repairSurvivors();
                }
            }
        }
    }

    /** Have all survivors supplied a full repair log? */
    public boolean areRepairLogsComplete()
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
        // cancel() and repair() must be synchronized by the caller (the deliver lock,
        // currently). If cancelled and the last repair message arrives, don't send
        // out corrections!
        if (this.m_promotionResult.isCancelled()) {
            tmLog.debug(m_whoami + "Skipping repair message creation for cancelled Term.");
            return;
        }

        int queued = 0;
        tmLog.info(m_whoami + "received all repair logs and is repairing surviving replicas.");
        for (Iv2RepairLogResponseMessage li : m_repairLogUnion) {
            List<Long> needsRepair = new ArrayList<Long>(5);
            for (Entry<Long, ReplicaRepairStruct> entry : m_replicaRepairStructs.entrySet()) {
                if  (entry.getValue().needs(li.getSpHandle())) {
                    ++queued;
                    tmLog.debug(m_whoami + "repairing " + entry.getKey() + ". Max seen " +
                            entry.getValue().m_maxSpHandleSeen + ". Repairing with " +
                            li.getSpHandle());
                    needsRepair.add(entry.getKey());
                }
            }
            if (!needsRepair.isEmpty()) {
                m_mailbox.repairReplicasWith(needsRepair, li);
            }
        }
        tmLog.info(m_whoami + "finished queuing " + queued + " replica repair messages.");

        // Can't run ZK work on a Network thread. Hack up a new context here.
        // See ENG-3176
        Thread declareLeaderThread = new Thread() {
            @Override
            public void run() {
                declareReadyAsLeader();
            }
        };
        declareLeaderThread.start();
    }



    // with leadership election complete, update the master list
    // for non-initiator components that care.
    void declareReadyAsLeader()
    {
        try {
            MapCacheWriter iv2masters = new MapCache(m_zk, VoltZK.iv2masters);
            iv2masters.put(Integer.toString(m_partitionId),
                    new JSONObject("{hsid:" + m_mailbox.getHSId() + "}"));
            m_promotionResult.m_doneLatch.countDown();
        } catch (KeeperException e) {
            VoltDB.crashLocalVoltDB("Bad news: failed to declare leader.", true, e);
        } catch (InterruptedException e) {
            VoltDB.crashLocalVoltDB("Bad news: failed to declare leader.", true, e);
        } catch (JSONException e) {
            VoltDB.crashLocalVoltDB("Bad news: failed to declare leader.", true, e);
        }
    }


}
