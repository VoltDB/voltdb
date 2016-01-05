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

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.PriorityBlockingQueue;

import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;

import com.google_voltpatches.common.collect.ImmutableMap;
import com.google_voltpatches.common.collect.Maps;

/**
 * This {@link SiteTaskerQueue} implements weighted fair queueing. In particular,
 * tasks can be assigned to different logical queues, and those logical queues
 * in turn can be assigned weights (integer percentage values, summing to less
 * than 100). These weights will be used to fairly interleave incoming tasks.
 *
 * Note that transactional work must be assigned to {@link #DEFAULT_QUEUE},
 * except on its initial arrival at the {@link InitiatorMailbox} of its partition
 * leader.
 *
 */
public class FairSiteTaskerQueue extends SiteTaskerQueue {
    private static final VoltLogger hostLog = new VoltLogger("HOST");

    /**
     * The queue identifiers below can be assigned to a {@link SiteTasker},
     * which will cause it to be routed to the corresponding logical queue.
     * Its weight may be specified using -DSITE_TASKER_$QUEUENAME_WEIGHT=[1,99].
     * All configured weights must add to no more than 99, and 100 minus this
     * sum will be the weight of the default queue.
     */
    public static enum SiteTaskerQueueType {
        DEFAULT_QUEUE,
        REPLICATION_WORK
    }
    private static final ImmutableMap<SiteTaskerQueueType, Double> QUEUE_INVERSE_WEIGHTS;

    static {
        EnumMap<SiteTaskerQueueType, Double> inverseWeights = new EnumMap<>(SiteTaskerQueueType.class);
        int weightOfDefault = 100;
        for (SiteTaskerQueueType q : SiteTaskerQueueType.values()) {
            if (q == SiteTaskerQueueType.DEFAULT_QUEUE) {
                continue;
            }
            String propName = "SITE_TASKER_" + q.name() + "_WEIGHT";
            Integer w = Integer.getInteger(propName);
            if (w != null) {
                if (w <= 0) {
                    hostLog.warn(propName + " must be greater than 0");
                } else if ((weightOfDefault - w) <= 0) {
                    hostLog.warn("Cannot accomodate " + propName + " value " + w +
                            "; all weights must sum to no more than 99");
                } else {
                    inverseWeights.put(q, weightInv(w));
                    weightOfDefault -= w;
                }
            }
        }
        inverseWeights.put(SiteTaskerQueueType.DEFAULT_QUEUE, weightInv(weightOfDefault));
        QUEUE_INVERSE_WEIGHTS = Maps.immutableEnumMap(inverseWeights);
    }

    private static double weightInv(int weight) {
        return 1.0 / (weight / 100.0);
    }

    /**
     * A logical, weighted clock for a single logical work queue. Each new task
     * finishes weight^-1 time units after the maximum of the last finish for
     * this queue, and the minimum currently enqueued finish across all queues.
     * ("Finish" here is used to denote a logical priority ordering between
     * tasks, rather than a wall clock time.) So, for example, if I have queue
     * with weight 20%, then every task costs 1/.2=5 time units.
     */
    private class VirtualTimer {
        final double m_weightInverse;
        private double m_lastVirtualTime;

        VirtualTimer(double weightInverse) {
            m_weightInverse = weightInverse;
        }

        double nextVirtualTime() {
            double virtualStart = m_lastVirtualTime;
            TaskWrapper w = m_tasks.peek();
            if (w != null) {
                virtualStart = Math.max(w.m_virtualTime, virtualStart);
            }
            return (m_lastVirtualTime = virtualStart + m_weightInverse);
        }
    }

    private static class TaskWrapper implements Comparable<TaskWrapper> {
        final double m_virtualTime;
        final SiteTasker m_task;

        TaskWrapper(double virtualTime, SiteTasker task) {
            m_virtualTime = virtualTime;
            m_task = task;
        }

        @Override
        public int compareTo(TaskWrapper o) {
            return (int)Math.signum(m_virtualTime - o.m_virtualTime);
        }
    }

    private final ImmutableMap<SiteTaskerQueueType, VirtualTimer> m_queueTimers;
    private final PriorityBlockingQueue<TaskWrapper> m_tasks = new PriorityBlockingQueue<>();

    public FairSiteTaskerQueue() {
        EnumMap<SiteTaskerQueueType, VirtualTimer> queueTimers = new EnumMap<>(SiteTaskerQueueType.class);
        for (Map.Entry<SiteTaskerQueueType, Double> e : QUEUE_INVERSE_WEIGHTS.entrySet()) {
            queueTimers.put(e.getKey(), new VirtualTimer(e.getValue()));
        }
        m_queueTimers = Maps.immutableEnumMap(queueTimers);
    }

    @Override
    public boolean offer(SiteTasker task) {
        VirtualTimer t = m_queueTimers.get(task.getQueueIdentifier());
        if (t == null) {
            // There is no weight mapped for this virtual queue; funnel the task
            // into the default queue
            t = m_queueTimers.get(SiteTaskerQueueType.DEFAULT_QUEUE);
            assert t != null;
        }
        synchronized (t) {
            return m_tasks.offer(new TaskWrapper(t.nextVirtualTime(), task));
        }
    }

    @Override
    public SiteTasker poll() {
        TaskWrapper wrapper = m_tasks.poll();
        if (wrapper != null) {
            return wrapper.m_task;
        }
        return null;
    }

    @Override
    public boolean isEmpty() {
        return m_tasks.isEmpty();
    }

    @Override
    protected SiteTasker takeImpl() throws InterruptedException {
        return CoreUtils.queueSpinTake(m_tasks).m_task;
    }

}
