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

import org.voltdb.LatencyBucketSet;

public class ClientStats {
    final String procName;
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

    final public static int ONE_MS_BUCKET_COUNT = 50;
    final public static int TEN_MS_BUCKET_COUNT = 20;
    final public static int HUNDRED_MS_BUCKET_COUNT = 10;

    final LatencyBucketSet latencyBy1ms;
    final LatencyBucketSet latencyBy10ms;
    final LatencyBucketSet latencyBy100ms;

    ClientStats() {
        procName = "";
        connectionId = -1;
        hostname = "";
        port = -1;
        since = Long.MAX_VALUE;
        invocationsCompleted = invocationAborts = invocationErrors = 0;
        roundTripTime = clusterRoundTripTime = 0;
        maxRoundTripTime = maxClusterRoundTripTime = 0;
        latencyBy1ms = new LatencyBucketSet(1, ONE_MS_BUCKET_COUNT);
        latencyBy10ms = new LatencyBucketSet(10, TEN_MS_BUCKET_COUNT);
        latencyBy100ms = new LatencyBucketSet(100, HUNDRED_MS_BUCKET_COUNT);
    }

    ClientStats(ProcedureStatsTracker stats, boolean interval, long since) {
        procName = stats.name;
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
        latencyBy1ms = (LatencyBucketSet) core.m_latencyBy1ms.clone();
        latencyBy10ms = (LatencyBucketSet) core.m_latencyBy10ms.clone();
        latencyBy100ms = (LatencyBucketSet) core.m_latencyBy100ms.clone();

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
        if (ps1.procName.equals(ps2.procName)) procName = ps1.procName;
        else procName = "";

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

        latencyBy1ms = LatencyBucketSet.merge(ps1.latencyBy1ms, ps2.latencyBy1ms);
        latencyBy10ms = LatencyBucketSet.merge(ps1.latencyBy10ms, ps2.latencyBy10ms);
        latencyBy100ms = LatencyBucketSet.merge(ps1.latencyBy100ms, ps2.latencyBy100ms);
    }

    public String getProcedureName() {
        return procName;
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
        return latencyBy1ms.buckets.clone();
    }

    public long[] getLatencyBucketsBy10ms() {
        return latencyBy10ms.buckets.clone();
    }

    public long[] getLatencyBucketsBy100ms() {
        return latencyBy100ms.buckets.clone();
    }

    public int kPercentileLatency(double percentile) {
        int kpl;

        kpl = latencyBy1ms.kPercentileLatency(percentile);
        if (kpl != Integer.MAX_VALUE) {
            return kpl;
        }

        kpl = latencyBy10ms.kPercentileLatency(percentile);
        if (kpl != Integer.MAX_VALUE) {
            return kpl;
        }

        return latencyBy100ms.kPercentileLatency(percentile);
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
                new Date(since).toString(), procName, connectionId));
        sb.append(String.format("    hostname: %s:%d\n",
                hostname, port));
        sb.append(String.format("    invocations completed/aborted/errors: %d/%d/%d\n",
                invocationsCompleted, invocationAborts, invocationErrors));
        if (invocationsCompleted > 0) {
            sb.append(String.format("    avg latency client/internal: %d/%d\n",
                    roundTripTime / invocationsCompleted, clusterRoundTripTime / invocationsCompleted));
            sb.append(String.format("    max latency client/internal: %d/%d\n",
                    maxRoundTripTime, maxClusterRoundTripTime));
            sb.append(latencyBy1ms).append("\n");
            sb.append(latencyBy10ms).append("\n");
            sb.append(latencyBy100ms).append("\n");
        }

        return sb.toString();
    }
}
