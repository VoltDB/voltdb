/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
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
package org.voltdb.utils;

import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.voltcore.logging.VoltLogger;

import java.util.concurrent.BlockingQueue;

/**
 * This is a wrapper so that jetty serving exceptions are not printed, response to json apis do get back with client response
 * so this should not affect response failures.
 */
public class HTTPQueuedThreadPool extends QueuedThreadPool {

    private static final VoltLogger m_log = new VoltLogger("HOST");

    public HTTPQueuedThreadPool(int maxThreads, int minThreads, int idleTimeout, BlockingQueue<Runnable> queue) {
        super(maxThreads, minThreads, idleTimeout, queue);
    }

    @Override
    protected void runJob(Runnable job) {
        try {
            super.runJob(job);
        } catch (Throwable t) {
            // Swallow it any procedure exceptions do get to response from client response.
            if (m_log.isDebugEnabled()) {
                m_log.debug("Failed to process http request.", t);
            }
        }
    }
}

