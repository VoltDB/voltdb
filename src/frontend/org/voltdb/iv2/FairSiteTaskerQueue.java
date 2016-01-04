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

import java.util.Map;
import java.util.concurrent.PriorityBlockingQueue;

import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;

import com.google_voltpatches.common.collect.ImmutableMap;
import com.google_voltpatches.common.util.concurrent.AtomicDouble;

public class FairSiteTaskerQueue extends SiteTaskerQueue {
    private static final VoltLogger hostLog = new VoltLogger("HOST");

    public static final int DEFAULT_QUEUE = Integer.MIN_VALUE;
    public static final int REPLICATION_WORK = 1;
    private static final ImmutableMap<Integer, Double> QUEUE_INVERSE_WEIGHTS;

    static {
        ImmutableMap.Builder<Integer, Double> b = ImmutableMap.builder();
        int defaultWeight = 100;
        int w = loadWeight("SITE_TASKER_REPLICATION_WORK_WEIGHT", defaultWeight);
        if (w > 0) {
            b.put(REPLICATION_WORK, weightInv(w));
            defaultWeight -= w;
        }
        b.put(DEFAULT_QUEUE, weightInv(defaultWeight));
        QUEUE_INVERSE_WEIGHTS = b.build();
    }

    private static int loadWeight(String propName, int defaultWeight) {
        Integer w = Integer.getInteger(propName);
        if (w != null) {
            if (w <= 0) {
                hostLog.warn(propName + " must be greater than 0");
            } else if ((defaultWeight - w) <= 0) {
                hostLog.warn("Cannot accomodate " + propName + " value " + w +
                        "; all weights must sum to no more than 99");
            } else {
                return w;
            }
        }
        return -1;
    }

    private static double weightInv(int weight) {
        return 1.0 / (weight / 100.0);
    }

    private static class VirtualTimer {
        final double m_weightInverse;
        final AtomicDouble m_lastVirtualTime = new AtomicDouble();

        VirtualTimer(double weightInverse) {
            m_weightInverse = weightInverse;
        }

        double nextVirtualTime() {
            double lastVirtualTime;
            double nextVirtualTime;
            do {
                lastVirtualTime = m_lastVirtualTime.get();
                double clockTime = System.currentTimeMillis() - UniqueIdGenerator.VOLT_EPOCH;
                double virtualStart = Math.max(clockTime, lastVirtualTime);
                nextVirtualTime = virtualStart + m_weightInverse;
            } while (!m_lastVirtualTime.compareAndSet(lastVirtualTime, nextVirtualTime));
            return nextVirtualTime;
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

    private final ImmutableMap<Integer, VirtualTimer> m_queueTimers;
    private final PriorityBlockingQueue<TaskWrapper> m_tasks = new PriorityBlockingQueue<>();

    public FairSiteTaskerQueue() {
        ImmutableMap.Builder<Integer, VirtualTimer> b = ImmutableMap.builder();
        for (Map.Entry<Integer, Double> e : QUEUE_INVERSE_WEIGHTS.entrySet()) {
            b.put(e.getKey(), new VirtualTimer(e.getValue()));
        }
        m_queueTimers = b.build();
    }

    @Override
    public boolean offer(SiteTasker task) {
        VirtualTimer t = m_queueTimers.get(task.getQueueIdentifier());
        if (t == null) {
            t = m_queueTimers.get(DEFAULT_QUEUE);
            assert t != null;
        }
        return m_tasks.offer(new TaskWrapper(t.nextVirtualTime(), task));
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
