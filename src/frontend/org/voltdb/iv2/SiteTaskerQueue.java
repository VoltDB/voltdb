/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

import org.voltcore.utils.CoreUtils;
import org.voltdb.StarvationTracker;

/** SiteTaskerScheduler orders SiteTaskers for execution. */
public abstract class SiteTaskerQueue {
    private static final boolean USE_FAIR_SCHEDULING = Boolean.getBoolean("SITE_TASKER_USE_FAIR_SCHEDULING");

    public static SiteTaskerQueue create() {
        if (USE_FAIR_SCHEDULING) {
            return new FairSiteTaskerQueue();
        }
        return new DefaultSiteTaskerQueue();
    }


    private StarvationTracker m_starvationTracker;

    // Block on the site tasker queue.
    public SiteTasker take() throws InterruptedException {
        SiteTasker task = poll();
        if (task == null) {
            m_starvationTracker.beginStarvation();
        } else {
            return task;
        }
        try {
            return takeImpl();
        } finally {
            m_starvationTracker.endStarvation();
        }
    }

    public void setStarvationTracker(StarvationTracker tracker) {
        m_starvationTracker = tracker;
    }

    public abstract boolean offer(SiteTasker task);

    public abstract SiteTasker poll();

    public abstract boolean isEmpty();

    protected abstract SiteTasker takeImpl() throws InterruptedException;

    static class DefaultSiteTaskerQueue extends SiteTaskerQueue {
        private final LinkedTransferQueue<SiteTasker> m_tasks = new LinkedTransferQueue<SiteTasker>();

        @Override
        public boolean offer(SiteTasker task) {
            return m_tasks.offer(task);
        }

        @Override
        public SiteTasker poll() {
            return m_tasks.poll();
        }

        @Override
        protected SiteTasker takeImpl() throws InterruptedException {
            return CoreUtils.queueSpinTake(m_tasks);
        }

        @Override
        public boolean isEmpty() {
            return m_tasks.isEmpty();
        }
    }
}
