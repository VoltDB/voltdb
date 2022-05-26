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

import java.io.IOException;

import org.voltdb.SiteProcedureConnection;
import org.voltdb.client.Priority;
import org.voltdb.rejoin.TaskLog;
import org.voltdb.utils.Prioritized;

public abstract class SiteTasker implements Comparable<SiteTasker>, Prioritized {

    private long queueOfferTime = -1L;
    /*
     * By default, the priority is the highest in the system and preserves
     * the ordering of all tasks created internally.
     */
    private int priority = Priority.SYSTEM_PRIORITY;

    public void setQueueOfferTime() {
        queueOfferTime = System.nanoTime();
    }

    public long getQueueOfferTime() {
        return queueOfferTime;
    }

    @Override
    public void setPriority(int prio) {
        priority = prio;
    }

    @Override
    public int getPriority() {
        return priority;
    }

    @Override
    public int compareTo(SiteTasker other) {
        return Integer.compare(this.priority, other.priority);
    }

    public static abstract class SiteTaskerRunnable extends SiteTasker {
        protected String taskInfo = "";
        abstract void run();

        @Override
        public void run(SiteProcedureConnection siteConnection) {
            run();
        }

        @Override
        public void runForRejoin(SiteProcedureConnection siteConnection,
                TaskLog rejoinTaskLog) throws IOException {
            run();
        }
        @Override
        public String getTaskInfo() {
            return taskInfo;
        }
    }

    /**
     * Run executes the task. Run is called on the ExecutionSite thread
     * and has exclusive access to the ee. Tasks are not preempted.
     */
    abstract public void run(SiteProcedureConnection siteConnection);

    /**
     * Run the task on an inconsistent/rejoining EE.
     */
    abstract public void runForRejoin(SiteProcedureConnection siteConnection,
            TaskLog rejoinTaskLog) throws IOException;

    public String getTaskInfo() {
        return getClass().getSimpleName();
    }
}
