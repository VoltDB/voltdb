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

import jsr166y.LinkedTransferQueue;

import org.voltdb.StarvationTracker;

/** SiteTaskerScheduler orders SiteTaskers for execution. */
public class SiteTaskerQueue
{
    private final LinkedTransferQueue<SiteTasker> m_tasks = new LinkedTransferQueue<SiteTasker>();
    private StarvationTracker m_starvationTracker;

    public boolean offer(SiteTasker task)
    {
        return m_tasks.offer(task);
    }

    public SiteTasker poll() throws InterruptedException
    {
        SiteTasker task = m_tasks.poll();
        if (task == null) {
            m_starvationTracker.beginStarvation();
        } else {
            return task;
        }
        try {
            return m_tasks.take();
        } finally {
            m_starvationTracker.endStarvation();
        }
    }

    public boolean isEmpty() {
        return m_tasks.isEmpty();
    }

    public void setStarvationTracker(StarvationTracker tracker) {
        m_starvationTracker = tracker;
    }
}
