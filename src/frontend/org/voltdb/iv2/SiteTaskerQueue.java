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

import java.util.concurrent.BlockingQueue;

import org.voltcore.utils.CoreUtils;
import org.voltdb.QueueDepthTracker;
import org.voltdb.StarvationTracker;

/** SiteTaskerScheduler orders SiteTaskers for execution. */
public class SiteTaskerQueue
{
    private final BlockingQueue<SiteTasker> m_tasks = PriorityPolicy.getSpTaskQueue();
    private StarvationTracker m_starvationTracker;
    private QueueDepthTracker m_queueDepthTracker;
    private int m_partitionId;

    public SiteTaskerQueue(int partitionId) {
        m_partitionId = partitionId;
    }

    public int getPartitionId() {
        return m_partitionId;
    }

    public boolean offer(SiteTasker task)
    {
        task.setQueueOfferTime();
        // update tracker before enqueue the task
        // prevent another thread from polling a task and decrementing
        // the queue depth before it is incremented
        // i.e. avoid queueDepth < 0
        m_queueDepthTracker.offerUpdate(task.getPriority());
        return m_tasks.offer(task);
    }

    // Block on the site tasker queue.
    public SiteTasker take() throws InterruptedException
    {
        SiteTasker task = m_tasks.poll();

        if (task == null) {
            m_starvationTracker.beginStarvation();
        } else {
            m_queueDepthTracker.pollUpdate(task.getQueueOfferTime(), task.getPriority());
            return task;
        }
        try {
            task = CoreUtils.queueSpinTake(m_tasks);
            // task is never null
            m_queueDepthTracker.pollUpdate(task.getQueueOfferTime(), task.getPriority());
            return task;
        } finally {
            m_starvationTracker.endStarvation();
        }
    }

    // Non-blocking poll on the site tasker queue.
    public SiteTasker poll()
    {
        SiteTasker task = m_tasks.poll();
        if (task != null) {
            m_queueDepthTracker.pollUpdate(task.getQueueOfferTime(), task.getPriority());
        }
        return task;
    }

    // Non-blocking peek on the site tasker queue.
    public SiteTasker peek()
    {
        return m_tasks.peek();
    }

    public boolean isEmpty() {
        return m_tasks.isEmpty();
    }

    public void setStarvationTracker(StarvationTracker tracker) {
        m_starvationTracker = tracker;
    }

    public QueueDepthTracker setupQueueDepthTracker(long siteId) {
        m_queueDepthTracker = new QueueDepthTracker(siteId, m_tasks);
        return m_queueDepthTracker;
    }

    public int size() {
        return m_tasks.size();
    }

    public void clear() {
        m_tasks.clear();
    }
}
