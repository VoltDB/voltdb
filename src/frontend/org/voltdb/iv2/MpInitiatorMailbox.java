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

import java.util.List;
import java.util.concurrent.CountDownLatch;

import jsr166y.LinkedTransferQueue;

import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.messaging.VoltMessage;

import com.google.common.base.Throwables;

/**
 * InitiatorMailbox accepts initiator work and proxies it to the
 * configured InitiationRole.
 */
public class MpInitiatorMailbox extends InitiatorMailbox
{
    VoltLogger hostLog = new VoltLogger("HOST");
    VoltLogger tmLog = new VoltLogger("TM");

    private final LinkedTransferQueue<Runnable> m_taskQueue = new LinkedTransferQueue<Runnable>();
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

    @Override
    public void setRepairAlgo(final RepairAlgo algo)
    {
        final CountDownLatch cdl = new CountDownLatch(1);
        m_taskQueue.offer(new Runnable() {
            @Override
            public void run() {
                try {
                    setRepairAlgoInternal(algo);
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
            RejoinProducer rejoinProducer)
    {
        super(partitionId, scheduler, messenger, repairLog, rejoinProducer);
        m_taskThread.start();
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
          m_taskThread.join();
      }


    @Override
    public void updateReplicas(final List<Long> replicas) {
        m_taskQueue.offer(new Runnable() {
            @Override
            public void run() {
                updateReplicasInternal(replicas);
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
}
