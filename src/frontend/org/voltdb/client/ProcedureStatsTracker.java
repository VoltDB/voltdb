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

package org.voltdb.client;

import org.voltdb.LatencyBucketSet;

public class ProcedureStatsTracker {

    class Stats {
        final long since; // java.util.Date compatible microseconds since epoch

        long m_invocationsCompleted = 0;
        long m_invocationAborts = 0;
        long m_invocationErrors = 0;

        // cumulative latency measured by client, used to calculate avg. lat.
        long m_roundTripTime = 0; // microsecs
        // cumulative latency measured by the cluster, used to calculate avg lat.
        long m_clusterRoundTripTime = 0; // microsecs

        int m_maxRoundTripTime = Integer.MIN_VALUE; // microsecs
        int m_maxClusterRoundTripTime = Integer.MIN_VALUE; // microsecs

        LatencyBucketSet m_latencyBy1ms =
                new LatencyBucketSet(1, ClientStats.ONE_MS_BUCKET_COUNT);
        LatencyBucketSet m_latencyBy10ms =
                new LatencyBucketSet(10, ClientStats.TEN_MS_BUCKET_COUNT);
        LatencyBucketSet m_latencyBy100ms =
                new LatencyBucketSet(100, ClientStats.HUNDRED_MS_BUCKET_COUNT);

        public Stats(long since) {
            this.since = since;
        }

        public void update(int roundTripTime, int clusterRoundTripTime,
                           boolean abort, boolean error)
        {
            m_maxRoundTripTime = Math.max(roundTripTime, m_maxRoundTripTime);
            m_maxClusterRoundTripTime = Math.max(clusterRoundTripTime, m_maxClusterRoundTripTime);

            m_invocationsCompleted++;
            if (abort) {
                m_invocationAborts++;
            }
            if (error) {
                m_invocationErrors++;
            }
            m_roundTripTime += roundTripTime;
            m_clusterRoundTripTime += clusterRoundTripTime;

            // calculate the latency buckets to increment and increment.
            m_latencyBy1ms.update(roundTripTime);
            m_latencyBy10ms.update(roundTripTime);
            m_latencyBy100ms.update(roundTripTime);
        }
    }

    final String name;
    final long connectionId;
    final String hostname;
    final int port;
    Stats m_lifetimeStats;
    Stats m_intervalStats;

    ProcedureStatsTracker(String name, long connectionId, String hostname, int port) {
        this.name = name;
        this.connectionId = connectionId;
        this.hostname = hostname;
        this.port = port;
        long now = System.currentTimeMillis();
        m_lifetimeStats = new Stats(now);
        m_intervalStats = new Stats(now);
    }

    void update(int roundTripTime, int clusterRoundTripTime, boolean abort, boolean error) {
        m_lifetimeStats.update(roundTripTime, clusterRoundTripTime, abort, error);
        m_intervalStats.update(roundTripTime, clusterRoundTripTime, abort, error);
    }

    void resetInterval(long since) {
        m_intervalStats = new Stats(since);
    }

    void resetAll(long since) {
        resetInterval(since);
        m_lifetimeStats = new Stats(since);
    }

}
