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

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;

import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.messaging.VoltMessage;
import org.voltdb.messaging.CompleteTransactionMessage;
import org.voltdb.messaging.Iv2InitiateTaskMessage;

import com.google_voltpatches.common.base.Supplier;
import com.google_voltpatches.common.base.Throwables;

/**
 * InitiatorMailbox accepts initiator work and proxies it to the
 * configured InitiationRole.
 */
public class MpInitiatorMailbox extends InitiatorMailbox
{
    VoltLogger hostLog = new VoltLogger("HOST");
    VoltLogger tmLog = new VoltLogger("TM");

    private final LinkedBlockingQueue<Runnable> m_taskQueue = new LinkedBlockingQueue<Runnable>();
    @SuppressWarnings("serial")
    private static class TerminateThreadException extends RuntimeException {};
    private long m_taskThreadId = 0;
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
    public RepairAlgo constructRepairAlgo(final Supplier<List<Long>> survivors, final String whoami) {
        RepairAlgo ra = null;
        if (Thread.currentThread().getId() != m_taskThreadId) {
            FutureTask<RepairAlgo> ft = new FutureTask(new Callable<RepairAlgo>() {
                @Override
                public RepairAlgo call() throws Exception {
                    RepairAlgo ra = new MpPromoteAlgo( survivors.get(), MpInitiatorMailbox.this, whoami);
                    setRepairAlgoInternal(ra);
                    return ra;
                }
            });
            m_taskQueue.offer(ft);
            try {
                ra = ft.get();
            } catch (Exception e) {
                Throwables.propagate(e);
            }
        } else {
            ra = new MpPromoteAlgo( survivors.get(), this, whoami);
            setRepairAlgoInternal(ra);
        }
        return ra;
    }

    @Override
    public void setLeaderState(final long maxSeenTxnId)
    {
        final CountDownLatch cdl = new CountDownLatch(1);
        m_taskQueue.offer(new Runnable() {
            @Override
            public void run() {
                try {
                    setLeaderStateInternal(maxSeenTxnId);
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
        m_taskQueue.offer(new Runnable() {
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
        m_taskQueue.offer(new Runnable() {
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
        m_taskQueue.offer(new Runnable() {
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
            Scheduler scheduler,
            HostMessenger messenger, RepairLog repairLog,
            JoinProducerBase rejoinProducer)
    {
        super(partitionId, scheduler, messenger, repairLog, rejoinProducer);
        m_taskThread.start();
        m_sendThread.start();
    }

      @Override
      public void shutdown() throws InterruptedException {
          m_taskQueue.offer(new Runnable() {
              @Override
              public void run() {
                  try {
                      shutdownInternal();
                  } catch (InterruptedException e) {
                      tmLog.info("Interrupted during shutdown", e);
                  }
              }
          });
          m_taskQueue.offer(new Runnable() {
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
    public void updateReplicas(final List<Long> replicas, final Map<Integer, Long> partitionMasters) {
        m_taskQueue.offer(new Runnable() {
            @Override
            public void run() {
                updateReplicasInternal(replicas, partitionMasters);
            }
        });
    }

    @Override
    public void deliver(final VoltMessage message) {
        m_taskQueue.offer(new Runnable() {
            @Override
            public void run() {
                deliverInternal(message);
            }
        });
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
            m_taskQueue.offer(new Runnable() {
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
            m_scheduler.updateLastSeenTxnIds(work);
            m_scheduler.handleMessageRepair(needsRepair, work);
        }
        else if (repairWork instanceof CompleteTransactionMessage) {
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
