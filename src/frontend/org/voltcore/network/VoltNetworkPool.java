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

package org.voltcore.network;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
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

    public VoltNetworkPool(int numThreads, ScheduledExecutorService ses) {
        m_networks = new VoltNetwork[numThreads];
        for (int ii = 0; ii < numThreads; ii++) {
            m_networks[ii] = new VoltNetwork(ii, ses);
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
        return registerChannel( channel, handler, SelectionKey.OP_READ);
    }

    public Connection registerChannel(
            final SocketChannel channel,
            final InputHandler handler,
            final int interestOps) throws IOException {
        VoltNetwork vn = m_networks[(int)(m_nextWorkerSelection.incrementAndGet() % m_networks.length)];
        return vn.registerChannel(channel, handler, interestOps);
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
                int ii = 0;
                for (long stat : stats.get(-1L).getSecond()) {
                    globalStats[ii] += stat;
                }
            }
            retval.putAll(stats);
        }
        retval.put(-1L, Pair.of("GLOBAL", globalStats));

        return retval;
    }
}
