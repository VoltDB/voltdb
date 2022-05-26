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

package org.voltcore.network;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import javax.net.ssl.SSLEngine;

import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.Pair;

public class VoltNetworkPool {

    public interface IOStatsIntf {
        Future<Map<Long, Pair<String, long[]>>> getIOStats(final boolean interval);
    }

    private static final VoltLogger m_logger = new VoltLogger(VoltNetworkPool.class.getName());

    private final VoltNetwork m_networks[];
    private final AtomicLong m_nextNetwork = new AtomicLong();
    public final String m_poolName;

    public VoltNetworkPool() {
        this(1, 1, null, "");
    }

    public VoltNetworkPool(int numThreads, int startThreadId, Queue<String> coreBindIds, String poolName) {
        m_poolName = poolName;
        if (numThreads < 1) {
            throw new IllegalArgumentException("Must specify a positive number of threads");
        }
        if (coreBindIds == null || coreBindIds.isEmpty()) {
            m_networks = new VoltNetwork[numThreads];
            for (int ii = 0; ii < numThreads; ii++) {
                // Adding startThreadId avoids unnecessary polling for non-Server VoltNetworkPools
                m_networks[ii] = new VoltNetwork(ii+startThreadId, null, poolName);
            }
        } else {
            final int coreBindIdsSize = coreBindIds.size();
            m_networks = new VoltNetwork[coreBindIdsSize];
            for (int ii = 0; ii < coreBindIdsSize; ii++) {
                // Adding startThreadId avoids unnecessary polling for non-Server VoltNetworkPools
                m_networks[ii] = new VoltNetwork(ii+startThreadId, coreBindIds.poll(), poolName);
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
            final InputHandler handler,
            final CipherExecutor cipherService,
            final SSLEngine sslEngine) throws IOException {
        return registerChannel( channel, handler, SelectionKey.OP_READ, ReverseDNSPolicy.ASYNCHRONOUS, cipherService, sslEngine);
    }

    public Connection registerChannel(
            final SocketChannel channel,
            final InputHandler handler,
            final int interestOps,
            final ReverseDNSPolicy dns,
            final CipherExecutor cipherService,
            final SSLEngine sslEngine) throws IOException {
        //Start with a round robin base policy
        VoltNetwork vn = m_networks[(int)(m_nextNetwork.getAndIncrement() % m_networks.length)];
        //Then do a load based policy which is a little racy
        for (int ii = 0; ii < m_networks.length; ii++) {
            if (m_networks[ii] == vn) continue;
            if (vn.numPorts() > m_networks[ii].numPorts()) {
                vn = m_networks[ii];
            }
        }
        return vn.registerChannel(channel, handler, interestOps, dns, cipherService, sslEngine);
    }

    public List<Long> getThreadIds() {
        ArrayList<Long> ids = new ArrayList<Long>();
        for (VoltNetwork vn : m_networks) {
            ids.add(vn.getThreadId());
        }
        return ids;
    }

    public Map<Long, Pair<String, long[]>>
        getIOStats(final boolean interval, List<IOStatsIntf> picoNetworks)
                throws ExecutionException, InterruptedException {
        HashMap<Long, Pair<String, long[]>> retval = new HashMap<Long, Pair<String, long[]>>();

        LinkedList<Future<Map<Long, Pair<String, long[]>>>> statTasks =
                new LinkedList<Future<Map<Long, Pair<String, long[]>>>>();
        for (VoltNetwork vn : m_networks) {
            statTasks.add(vn.getIOStats(interval));
        }
        for (IOStatsIntf pn : picoNetworks) {
            statTasks.add(pn.getIOStats(interval));
        }

        long globalStats[] = null;
        for (Future<Map<Long, Pair<String, long[]>>> statsFuture : statTasks) {
            try {
                Map<Long, Pair<String, long[]>> stats = statsFuture.get(500, TimeUnit.MILLISECONDS);
                if (globalStats == null) {
                    globalStats = stats.get(-1L).getSecond();
                } else {
                    final long localStats[] = stats.get(-1L).getSecond();
                    for (int ii = 0; ii < localStats.length; ii++) {
                        globalStats[ii] += localStats[ii];
                    }
                }
                retval.putAll(stats);
            } catch (TimeoutException e) {
                m_logger.warn("Timed out retrieving stats from network thread, probably harmless", e);
            }
        }
        retval.put(-1L, Pair.of("GLOBAL", globalStats));

        return retval;
    }

    public Set<Connection> getConnections() {
        List<Future<Set<Connection>>> futures = new ArrayList<>(m_networks.length);
        for (VoltNetwork vn : m_networks) {
            futures.add(vn.getConnections());
        }
        Set<Connection> conns = new HashSet<>();
        for (Future<Set<Connection>> fut : futures) {
            Set<Connection> connsForNetwork;
            try {
                connsForNetwork = fut.get();
            } catch (InterruptedException | ExecutionException e) {
                connsForNetwork = new HashSet<>();
            }
            conns.addAll(connsForNetwork);
        }
        return conns;
    }
}
