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

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

import org.voltcore.messaging.HostMessenger;
import org.voltcore.messaging.VoltMessage;
import org.voltdb.client.Priority;
import org.voltdb.dtxn.TransactionState;
import org.voltdb.iv2.MpTerm.RepairType;
import org.voltdb.messaging.CompleteTransactionMessage;
import org.voltdb.messaging.Iv2InitiateTaskMessage;
import org.voltdb.utils.Prioritized;

import com.google_voltpatches.common.base.Supplier;
import com.google_voltpatches.common.base.Throwables;

/**
 * MpInitiatorMailbox accepts MP initiator work and proxies it to the
 * configured InitiationRole. Uses 2 threads:
 * <ul>
 * <li>MpInitiator deliver runs tasks initiating MPs, repairing MPs, transitioning MP intiator leadership</li>
 * <li>MPInitiator send runs tasks delivering completions</li>
 * </ul>
 */
public class MpInitiatorMailbox extends InitiatorMailbox
{
    // A class for MPI tasks: needs a settable priority
    static abstract class MpiTask implements Runnable, Comparable<MpiTask>, Prioritized {

        private int priority = Priority.SYSTEM_PRIORITY;

        @Override
        public int compareTo(MpiTask other) {
            return Integer.compare(this.priority, other.priority);
        }

        @Override
        public void setPriority(int prio) {
            priority = prio;
        }

        @Override
        public int getPriority() {
            return priority;
        }
    }
    private final BlockingQueue<MpiTask> m_taskQueue = PriorityPolicy.getMpTaskQueue();

    @SuppressWarnings("serial")
    private static class TerminateThreadException extends RuntimeException {};
    private long m_taskThreadId = 0;
    private final MpRestartSequenceGenerator m_restartSeqGenerator;
    private final Thread m_taskThread = new Thread(null,
                    new Runnable() {
                        @Override
                        public void run() {
                            m_taskThreadId = Thread.currentThread().getId();
                            while (true) {
                                try {
                                    m_taskQueue.take().run();
                                } catch (TerminateThreadException e) {
                                    break;
                                } catch (Exception e) {
                                    tmLog.error("Unexpected exception in MpInitiator deliver thread", e);
                                }
                            }
                        }
                    },
                    "MpInitiator deliver", 1024 * 128);

    private final LinkedBlockingQueue<Runnable> m_sendQueue = new LinkedBlockingQueue<Runnable>();
    private final Thread m_sendThread = new Thread(null,
                    new Runnable() {
                        @Override
                        public void run() {
                            while (true) {
                                try {
                                    m_sendQueue.take().run();
                                } catch (TerminateThreadException e) {
                                    break;
                                } catch (Exception e) {
                                    tmLog.error("Unexpected exception in MpInitiator send thread", e);
                                }
                            }
                        }
                    },
                    "MpInitiator send", 1024 * 128);

    @Override
    public RepairAlgo constructRepairAlgo(final Supplier<List<Long>> survivors, int deadHost, final String whoami, boolean skipTxnRestart) {
        if (Thread.currentThread().getId() != m_taskThreadId) {
            final AtomicReference<RepairAlgo> ra = new AtomicReference<>();
            final CountDownLatch cdl = new CountDownLatch(1);

            m_taskQueue.offer(new MpiTask() {
                @Override
                public void run() {
                    try {
                        RepairAlgo ralgo = new MpPromoteAlgo(survivors.get(), deadHost, MpInitiatorMailbox.this,
                                m_restartSeqGenerator, whoami, skipTxnRestart);
                        setRepairAlgoInternal(ralgo);
                        ra.set(ralgo);
                    } finally {
                        cdl.countDown();
                    }
                }});
            try {
                cdl.await();
            } catch (Exception e) {
                Throwables.propagate(e);
            }
            return ra.get();
        } else {
            RepairAlgo ra = new MpPromoteAlgo(survivors.get(), deadHost, this, m_restartSeqGenerator, whoami, skipTxnRestart);
            setRepairAlgoInternal(ra);
            return ra;
        }
    }

    public void setLeaderState(final long maxSeenTxnId, final long repairTruncationHandle)
    {
        final CountDownLatch cdl = new CountDownLatch(1);
        m_taskQueue.offer(new MpiTask() {
            @Override
            public void run() {
                try {
                    setLeaderStateInternal(maxSeenTxnId);
                    ((MpScheduler)m_scheduler).m_repairLogTruncationHandle = repairTruncationHandle;
                    ((MpScheduler)m_scheduler).m_repairLogAwaitingTruncate = repairTruncationHandle;
                } finally {
                    cdl.countDown();
                }
            }
        });
        try {
            cdl.await();
        } catch (InterruptedException e) {
            Throwables.propagate(e);
        }
    }

    @Override
    public void setMaxLastSeenMultipartTxnId(final long txnId) {
        final CountDownLatch cdl = new CountDownLatch(1);
        m_taskQueue.offer(new MpiTask() {
            @Override
            public void run() {
                try {
                    setMaxLastSeenMultipartTxnIdInternal(txnId);
                } finally {
                    cdl.countDown();
                }
            }
        });
        try {
            cdl.await();
        } catch (InterruptedException e) {
            Throwables.propagate(e);
        }
    }


    @Override
    public void setMaxLastSeenTxnId(final long txnId) {
        final CountDownLatch cdl = new CountDownLatch(1);
        m_taskQueue.offer(new MpiTask() {
            @Override
            public void run() {
                try {
                    setMaxLastSeenTxnIdInternal(txnId);
                } finally {
                    cdl.countDown();
                }
            }
        });
        try {
            cdl.await();
        } catch (InterruptedException e) {
            Throwables.propagate(e);
        }
    }

    @Override
    public void enableWritingIv2FaultLog() {
        final CountDownLatch cdl = new CountDownLatch(1);
        m_taskQueue.offer(new MpiTask() {
            @Override
            public void run() {
                try {
                    enableWritingIv2FaultLogInternal();
                } finally {
                    cdl.countDown();
                }
            }
        });
        try {
            cdl.await();
        } catch (InterruptedException e) {
            Throwables.propagate(e);
        }
    }


    public MpInitiatorMailbox(int partitionId,
            MpScheduler scheduler,
            HostMessenger messenger, RepairLog repairLog,
            JoinProducerBase rejoinProducer)
    {
        super(partitionId, scheduler, messenger, repairLog, rejoinProducer);
        m_restartSeqGenerator = new MpRestartSequenceGenerator(scheduler.getLeaderId(), false);
        m_taskThread.start();
        m_sendThread.start();
    }

    @Override
    public void shutdown() throws InterruptedException {
        m_taskQueue.offer(new MpiTask() {
            @Override
            public void run() {
                try {
                    shutdownInternal();
                } catch (InterruptedException e) {
                    tmLog.info("Interrupted during shutdown", e);
                }
            }
        });
        m_taskQueue.offer(new MpiTask() {
            @Override
            public void run() {
                throw new TerminateThreadException();
            }
        });
        m_sendQueue.offer(new Runnable() {
            @Override
            public void run() {
                throw new TerminateThreadException();
            }
        });
        m_taskThread.join();
        m_sendThread.join();
    }

    @Override
    public long[] updateReplicas(final List<Long> replicas, final Map<Integer, Long> partitionMasters,
            TransactionState snapshotTransactionState) {
        m_taskQueue.offer(new MpiTask() {
            @Override
            public void run() {
                updateReplicasInternal(replicas, partitionMasters, snapshotTransactionState);
            }
        });
        return new long[0];
    }

    public void updateReplicas(final List<Long> replicas, final Map<Integer, Long> partitionMasters, RepairType repairType) {
        m_taskQueue.offer(new MpiTask() {
            @Override
            public void run() {
                assert(lockingVows());
                Iv2Trace.logTopology(getHSId(), replicas, m_partitionId);
                // If leader change is caused by leader migration request,
                // and no pending repair was cancelled previously, the repair
                // message is not necessarily needed
                RepairType type = repairType;
                if (repairType.isMigrate() && m_algo != null && m_algo.isCancelled()) {
                    type = RepairType.SKIP_MP_REPAIR;
                }
                // If a replica set has been configured and it changed during
                // promotion, must cancel the term
                if (!type.isMigrate() && m_algo != null) {
                    m_algo.cancel();
                }
                ((MpScheduler)m_scheduler).updateReplicas(replicas, partitionMasters, type);
            }
        });
    }

    @Override
    public void deliver(final VoltMessage message) {
        // Offer a task but set priority from Iv2InitiateTaskMessage
        m_taskQueue.offer(new MpiTask() {
            @Override
            public void run() {
                deliverInternal(message);
            }
            private MpiTask init(VoltMessage message) {
                if (message instanceof Iv2InitiateTaskMessage
                        && ((Iv2InitiateTaskMessage) message).getStoredProcedureInvocation() != null) {
                    setPriority(((Iv2InitiateTaskMessage) message).getStoredProcedureInvocation().getRequestPriority());
                }
                return this;
            }
        }.init(message));
    }

    @Override
    void repairReplicasWith(final List<Long> needsRepair, final VoltMessage repairWork)
    {
        if (Thread.currentThread().getId() == m_taskThreadId) {
            //When called from MpPromoteAlgo which should be entered from deliver
            repairReplicasWithInternal(needsRepair, repairWork);
        } else {
            //When called from MpInitiator.acceptPromotion
            final CountDownLatch cdl = new CountDownLatch(1);
            m_taskQueue.offer(new MpiTask() {
                @Override
                public void run() {
                    try {
                        repairReplicasWithInternal( needsRepair, repairWork);
                    } finally {
                        cdl.countDown();
                    }
                }
            });
            try {
                cdl.await();
            } catch (InterruptedException e) {
                Throwables.propagate(e);
            }
        }

    }

    private void repairReplicasWithInternal(List<Long> needsRepair, VoltMessage repairWork) {
        assert(lockingVows());
        if (repairWork instanceof Iv2InitiateTaskMessage) {
            Iv2InitiateTaskMessage m = (Iv2InitiateTaskMessage)repairWork;
            Iv2InitiateTaskMessage work = new Iv2InitiateTaskMessage(m.getInitiatorHSId(), getHSId(), m);
            m_scheduler.updateLastSeenUniqueIds(work);
            m_scheduler.handleMessageRepair(needsRepair, work);
        }
        else if (repairWork instanceof CompleteTransactionMessage) {
            ((CompleteTransactionMessage) repairWork).setForReplica(false);
            send(com.google_voltpatches.common.primitives.Longs.toArray(needsRepair), repairWork);
        }
        else {
            throw new RuntimeException("During MPI repair: Invalid repair message type: " + repairWork);
        }
    }

    // This will be called from the internal task thread, deliver->deliverInternal->handleInitiateResponse
    // when the MpScheduler needs to log the completion of a transaction to its local repair log
    void deliverToRepairLog(VoltMessage msg) {
        assert(Thread.currentThread().getId() == m_taskThreadId);
        m_repairLog.deliver(msg);
    }

    // Change the send() behavior for the MPI's mailbox so that
    // messages sent from multiple read-only sites will
    // have a serialized order to all hosts.
    private void sendInternal(long destHSId, VoltMessage message)
    {
        message.m_sourceHSId = getHSId();
        m_messenger.send(destHSId, message);
    }

    @Override
    public void send(final long destHSId, final VoltMessage message)
    {
        m_sendQueue.offer(new Runnable() {
            @Override
            public void run() {
                sendInternal(destHSId, message);
            }
        });
    }

    private void sendInternal(long[] destHSIds, VoltMessage message)
    {
        message.m_sourceHSId = getHSId();
        m_messenger.send(destHSIds, message);
    }

    @Override
    public void send(final long[] destHSIds, final VoltMessage message)
    {
        m_sendQueue.offer(new Runnable() {
            @Override
            public void run() {
                sendInternal(destHSIds, message);
            }
        });
    }
}
