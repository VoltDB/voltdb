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

package org.voltdb;

import java.lang.Object;

import java.util.Comparator;
import java.util.PriorityQueue;

/** SiteTaskerScheduler orders SiteTaskers for execution. */
public class SiteTaskerScheduler
{
    /** TaskComparator orders SiteTaskers by priority */
    static class TaskComparator implements Comparator<SiteTasker>
    {
        public int compare(SiteTasker o1, SiteTasker o2)
        {
            int priorityDiff = o1.priority() - o2.priority();
            if (priorityDiff == 0) {
                return (int)(o1.seq() - o2.seq());
            }
            else {
                return priorityDiff;
            }
        }

        public boolean equals(Object rhs)
        {
            return (rhs instanceof TaskComparator);
        }
    }

    final PriorityQueue<SiteTasker> m_tasks;
    final TaskComparator m_comparator = new TaskComparator();
    final int m_initialCap = 100;
    long m_sequence = 0;

    SiteTaskerScheduler()
    {
        m_tasks = new PriorityQueue<SiteTasker>(m_initialCap, m_comparator);
    }

    synchronized public boolean offer(SiteTasker task)
    {
        task.setSeq(++m_sequence);
        return m_tasks.offer(task);
    }

    synchronized public SiteTasker poll()
    {
        return m_tasks.poll();
    }
}
