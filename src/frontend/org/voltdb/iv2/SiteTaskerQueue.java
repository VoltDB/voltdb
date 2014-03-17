/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TimeUnit;

import org.voltdb.StarvationTracker;

/** SiteTaskerScheduler orders SiteTaskers for execution. */
public class SiteTaskerQueue
{
    public static final long SITE_SPIN_MICROS = Long.getLong("SITE_SPIN_MICROS", 0);
    private final LinkedTransferQueue<SiteTasker> m_tasks = new LinkedTransferQueue<SiteTasker>();
    private StarvationTracker m_starvationTracker;

    public boolean offer(SiteTasker task)
    {
        return m_tasks.offer(task);
    }

    // Block on the site tasker queue.
    public SiteTasker take() throws InterruptedException
    {
        SiteTasker task = m_tasks.poll();
        if (task == null) {
            m_starvationTracker.beginStarvation();
        } else {
            return task;
        }
        try {
            if (SITE_SPIN_MICROS > 0) {
                task = m_tasks.poll(SITE_SPIN_MICROS, TimeUnit.MICROSECONDS);
                if (task == null) {
                    task = m_tasks.take();
                }
                return task;
            } else {
                return m_tasks.take();
            }
        } finally {
            m_starvationTracker.endStarvation();
        }
    }

    // Non-blocking poll on the site tasker queue.
    public SiteTasker poll()
    {
        return m_tasks.poll();
    }

    public boolean isEmpty() {
        return m_tasks.isEmpty();
    }

    public void setStarvationTracker(StarvationTracker tracker) {
        m_starvationTracker = tracker;
    }
}
