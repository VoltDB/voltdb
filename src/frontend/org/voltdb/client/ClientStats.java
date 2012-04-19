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

package org.voltdb.client;

import java.util.Date;
import java.util.Iterator;

public class ClientStats {
    final String name;
    final long since; // java.util.Date compatible microseconds since epoch

    public final long connectionId;
    public final String hostname;
    public final int port;

    final long invocationsCompleted;
    final long invocationAborts;
    final long invocationErrors;

    // cumulative latency measured by client, used to calculate avg. lat.
    final long roundTripTime; // microsecs
    // cumulative latency measured by the cluster, used to calculate avg lat.
    final long clusterRoundTripTime; // microsecs

    final int maxRoundTripTime; // microsecs
    final int maxClusterRoundTripTime; // microsecs

    public static final int NUMBER_OF_BUCKETS = 10;

    final long latencyBy1ms[] = new long[NUMBER_OF_BUCKETS];
    final long latencyBy10ms[] = new long[NUMBER_OF_BUCKETS];
    final long latencyBy100ms[] = new long[NUMBER_OF_BUCKETS];

    ClientStats() {
        name = "";
        connectionId = -1;
        hostname = "";
        port = -1;
        since = Long.MAX_VALUE;
        invocationsCompleted = invocationAborts = invocationErrors = 0;
        roundTripTime = clusterRoundTripTime = 0;
        maxRoundTripTime = maxClusterRoundTripTime = 0;
    }

    ClientStats(ProcedureStatsTracker stats, boolean interval, long since) {
        name = stats.name;
        connectionId = stats.connectionId;
        hostname = stats.hostname;
        port = stats.port;

        ProcedureStatsTracker.Stats core = interval ?
                stats.m_intervalStats : stats.m_lifetimeStats;

        this.since = core.since;
        invocationsCompleted = core.m_invocationsCompleted;
        invocationAborts = core.m_invocationAborts;
        invocationErrors = core.m_invocationErrors;
        roundTripTime = core.m_roundTripTime;
        clusterRoundTripTime = core.m_clusterRoundTripTime;
        maxRoundTripTime = core.m_maxRoundTripTime;
        maxClusterRoundTripTime = core.m_maxClusterRoundTripTime;
        for (int i = 0; i < NUMBER_OF_BUCKETS; ++i) {
            latencyBy1ms[i] = core.m_latencyBy1ms[i];
            latencyBy10ms[i] = core.m_latencyBy10ms[i];
            latencyBy100ms[i] = core.m_latencyBy100ms[i];
        }

        if (interval) {
            stats.resetInterval(since);
        }
    }

    static ClientStats merge(Iterable<ClientStats> statsIterable) {
        return merge(statsIterable.iterator());
    }

    static ClientStats merge(Iterator<ClientStats> statsIter) {
        // empty set
        if (!statsIter.hasNext()) {
            return new ClientStats();
        }

        // seed the grouping by the first element
        ClientStats seed = statsIter.next();
        assert(seed != null);

        // add in all the other elements
        while (statsIter.hasNext()) {
            seed = new ClientStats(seed, statsIter.next());
        }
        return seed;
    }

    ClientStats(ClientStats ps1, ClientStats ps2) {
        if (ps1.name.equals(ps2.name)) name = ps1.name;
        else name = "";

        if (ps1.connectionId == ps2.connectionId) connectionId = ps1.connectionId;
        else connectionId = -1;

        if (ps1.hostname.equals(ps2.hostname)) hostname = ps1.hostname;
        else hostname = "";

        if (ps1.port == ps2.port) port = ps1.port;
        else port = -1;

        since = Math.min(ps1.since, ps2.since);

        invocationsCompleted = ps1.invocationsCompleted + ps2.invocationsCompleted;
        invocationAborts = ps1.invocationAborts + ps2.invocationAborts;
        invocationErrors = ps1.invocationErrors + ps2.invocationErrors;

        roundTripTime = ps1.roundTripTime + ps2.roundTripTime;
        clusterRoundTripTime = ps1.clusterRoundTripTime + ps2.clusterRoundTripTime;

        maxRoundTripTime = Math.max(ps1.maxRoundTripTime, ps2.maxRoundTripTime);
        maxClusterRoundTripTime = Math.max(ps1.maxClusterRoundTripTime, ps2.maxClusterRoundTripTime);

        for (int i = 0; i < NUMBER_OF_BUCKETS; ++i) {
            latencyBy1ms[i] = ps1.latencyBy1ms[i] + ps2.latencyBy1ms[i];
            latencyBy10ms[i] = ps1.latencyBy10ms[i] + ps2.latencyBy10ms[i];
            latencyBy100ms[i] = ps1.latencyBy100ms[i] + ps2.latencyBy100ms[i];
        }
    }

    public String getProcedureName() {
        return name;
    }

    public long getStartTimestamp() {
        return since;
    }

    public long getConnectionId() {
        return connectionId;
    }

    public String getHostname() {
        return hostname;
    }

    public int getPort() {
        return port;
    }

    public long getInvocationsCompleted() {
        return invocationsCompleted;
    }

    public long getInvocationAborts() {
        return invocationAborts;
    }

    public long getInvocationErrors() {
        return invocationErrors;
    }

    public long getAverageLatency() {
        return roundTripTime / invocationsCompleted;
    }

    public long getAverageInternalLatency() {
        return clusterRoundTripTime / invocationsCompleted;
    }

    public int maxLatency() {
        return maxRoundTripTime;
    }

    public int maxIntraClusterLatency() {
        return maxClusterRoundTripTime;
    }

    public long[] getLatencyBucketsBy1ms() {
        return latencyBy1ms.clone();
    }

    public long[] getLatencyBucketsBy10ms() {
        return latencyBy10ms.clone();
    }

    public long[] getLatencyBucketsBy100ms() {
        return latencyBy100ms.clone();
    }

    public int kPercentileLatency(double percentile) {
        if ((percentile >= 1.0) || (percentile < 0.0)) {
            throw new RuntimeException(
                    "KPercentileLatency accepts values greater than 0 and less than 1");
        }
        if (invocationsCompleted == 0) return -1;

        // find the number of calls with less than percentile latency
        long k = (long) (invocationsCompleted * percentile);
        if (k == 0) ++k; // ensure k=0 gives min latency

        long sum = 0;
        // check if the latency requested is in the 0-100ms bins
        if (k <= latencyBy100ms[0]) {

            // check if the latency requested is in the 0-10ms bins
            if (k <= latencyBy10ms[0]) {
                // sum up the counts in the bins until k is subsumed
                for (int i = 0; i < NUMBER_OF_BUCKETS; i++) {
                    sum += latencyBy1ms[i];
                    if (sum >= k) {
                        return i+1;
                    }
                }
                // should have found it by now
                assert(false);
            }

            // sum up the counts in the bins until k is subsumed
            for (int i = 0; i < NUMBER_OF_BUCKETS; i++) {
                sum += latencyBy10ms[i];
                if (sum >= k) {
                    return i * 10 + 5;
                }
            }
            // should have found it by now
            assert(false);
        }

        // sum up the counts in the bins until k is subsumed
        for (int i = 0; i < NUMBER_OF_BUCKETS; i++) {
            sum += latencyBy100ms[i];
            if (sum >= k) {
                return i * 100 + 50;
            }
        }

        // too much latency
        return Integer.MAX_VALUE;
    }

    public long getThroughput(long now) {
        if (now < since) {
            now = since + 1; // 1 ms duration is sorta cheatin'
        }
        long durationMs = now - since;
        return (long) (invocationsCompleted / (durationMs / 1000.0));
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("Since %s - Procedure: %s - ConnectionId: %d {\n",
                new Date(since).toString(), name, connectionId));
        sb.append(String.format("    hostname: %s:%d\n",
                hostname, port));
        sb.append(String.format("    invocations completed/aborted/errors: %d/%d/%d\n",
                invocationsCompleted, invocationAborts, invocationErrors));
        if (invocationsCompleted > 0) {
            sb.append(String.format("    avg latency client/internal: %d/%d\n",
                    roundTripTime / invocationsCompleted, clusterRoundTripTime / invocationsCompleted));
            sb.append(String.format("    max latency client/internal: %d/%d\n",
                    maxRoundTripTime, maxClusterRoundTripTime));
            sb.append("    0-10ms by 1ms:\n      [");
            for (int i = 0; i < NUMBER_OF_BUCKETS; i++) {
                sb.append(latencyBy1ms[i]);
                if (i == (NUMBER_OF_BUCKETS - 1)) {
                    sb.append("]\n");
                }
                else {
                    sb.append(", ");
                }
            }
            sb.append("    0-100ms by 10ms:\n      [");
            for (int i = 0; i < NUMBER_OF_BUCKETS; i++) {
                sb.append(latencyBy10ms[i]);
                if (i == (NUMBER_OF_BUCKETS - 1)) {
                    sb.append("]\n");
                }
                else {
                    sb.append(", ");
                }
            }
            sb.append("    0-1000ms by 100ms:\n      [");
            for (int i = 0; i < NUMBER_OF_BUCKETS; i++) {
                sb.append(latencyBy100ms[i]);
                if (i == (NUMBER_OF_BUCKETS - 1)) {
                    sb.append("]\n");
                }
                else {
                    sb.append(", ");
                }
            }
        }

        return sb.toString();
    }
}
