/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

package org.voltcore.network;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.Pair;

public class VoltNetworkPool {
    private static final VoltLogger m_logger = new VoltLogger(VoltNetworkPool.class.getName());
    private static final VoltLogger networkLog = new VoltLogger("NETWORK");

    private final VoltNetwork m_networks[];
    private final AtomicLong m_nextWorkerSelection = new AtomicLong();

    public VoltNetworkPool() {
        this(1, null);
    }

    public VoltNetworkPool(int numThreads, Queue<String> coreBindIds) {
        if (numThreads < 1) {
            throw new IllegalArgumentException("Must specify a postive number of threads");
        }
        if (coreBindIds == null || coreBindIds.isEmpty()) {
            m_networks = new VoltNetwork[numThreads];
            for (int ii = 0; ii < numThreads; ii++) {
                m_networks[ii] = new VoltNetwork(ii, null);
            }
        } else {
            final int coreBindIdsSize = coreBindIds.size();
            m_networks = new VoltNetwork[coreBindIdsSize];
            for (int ii = 0; ii < coreBindIdsSize; ii++) {
                m_networks[ii] = new VoltNetwork(ii, coreBindIds.poll());
            }
        }
    }

    public void start() {
        for (VoltNetwork vn : m_networks) {
            vn.start();
        }
    }

    public void shutdown() throws InterruptedException {
        for (VoltNetwork vn : m_networks) {
            vn.shutdown();
        }
    }

    public Connection registerChannel(
            final SocketChannel channel,
            final InputHandler handler) throws IOException {
        return registerChannel( channel, handler, SelectionKey.OP_READ, ReverseDNSPolicy.ASYNCHRONOUS);
    }

    public Connection registerChannel(
            final SocketChannel channel,
            final InputHandler handler,
            final int interestOps,
            final ReverseDNSPolicy dns) throws IOException {
        VoltNetwork vn = m_networks[(int)(m_nextWorkerSelection.incrementAndGet() % m_networks.length)];
        return vn.registerChannel(channel, handler, interestOps, dns);
    }

    public List<Long> getThreadIds() {
        ArrayList<Long> ids = new ArrayList<Long>();
        for (VoltNetwork vn : m_networks) {
            ids.add(vn.getThreadId());
        }
        return ids;
    }

    public Map<Long, Pair<String, long[]>>
        getIOStats(final boolean interval)
                throws ExecutionException, InterruptedException {
        HashMap<Long, Pair<String, long[]>> retval = new HashMap<Long, Pair<String, long[]>>();

        LinkedList<Future<Map<Long, Pair<String, long[]>>>> statTasks =
                new LinkedList<Future<Map<Long, Pair<String, long[]>>>>();
        for (VoltNetwork vn : m_networks) {
            statTasks.add(vn.getIOStats(interval));
        }

        long globalStats[] = null;
        for (Future<Map<Long, Pair<String, long[]>>> statsFuture : statTasks) {
            Map<Long, Pair<String, long[]>> stats = statsFuture.get();
            if (globalStats == null) {
                globalStats = stats.get(-1L).getSecond();
            } else {
                final long localStats[] = stats.get(-1L).getSecond();
                for (int ii = 0; ii < localStats.length; ii++) {
                    globalStats[ii] += localStats[ii];
                }
            }
            retval.putAll(stats);
        }
        retval.put(-1L, Pair.of("GLOBAL", globalStats));

        return retval;
    }
}
