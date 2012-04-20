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
    String m_procName;
    long m_since; // java.util.Date compatible microseconds since epoch

    long m_connectionId;
    String m_hostname;
    int m_port;

    long m_invocationsCompleted;
    long m_invocationAborts;
    long m_invocationErrors;

    // cumulative latency measured by client, used to calculate avg. lat.
    long m_roundTripTime; // microsecs
    // cumulative latency measured by the cluster, used to calculate avg lat.
    long m_clusterRoundTripTime; // microsecs

    final public static int ONE_MS_BUCKET_COUNT = 50;
    final public static int TEN_MS_BUCKET_COUNT = 20;
    final public static int HUNDRED_MS_BUCKET_COUNT = 10;

    LatencyBucketSet m_latencyBy1ms;
    LatencyBucketSet m_latencyBy10ms;
    LatencyBucketSet m_latencyBy100ms;

    long m_bytesSent;
    long m_bytesReceived;

    ClientStats() {
        m_procName = "";
        m_connectionId = -1;
        m_hostname = "";
        m_port = -1;
        m_since = Long.MAX_VALUE;
        m_invocationsCompleted = m_invocationAborts = m_invocationErrors = 0;
        m_roundTripTime = m_clusterRoundTripTime = 0;
        m_latencyBy1ms = new LatencyBucketSet(1, ONE_MS_BUCKET_COUNT);
        m_latencyBy10ms = new LatencyBucketSet(10, TEN_MS_BUCKET_COUNT);
        m_latencyBy100ms = new LatencyBucketSet(100, HUNDRED_MS_BUCKET_COUNT);
        m_bytesSent = m_bytesReceived = 0;
    }

    ClientStats(ClientStats other) {
        m_procName = other.m_procName;
        m_connectionId = other.m_connectionId;
        m_hostname = other.m_hostname;
        m_port = other.m_port;
        m_since = other.m_since;
        m_invocationsCompleted = other.m_invocationsCompleted;
        m_invocationAborts = other.m_invocationAborts;
        m_invocationErrors = other.m_invocationErrors;
        m_roundTripTime = other.m_roundTripTime;
        m_clusterRoundTripTime = other.m_clusterRoundTripTime;
        m_latencyBy1ms = (LatencyBucketSet) other.m_latencyBy1ms.clone();
        m_latencyBy10ms = (LatencyBucketSet) other.m_latencyBy10ms.clone();
        m_latencyBy100ms = (LatencyBucketSet) other.m_latencyBy100ms.clone();
        m_bytesSent = other.m_bytesSent;
        m_bytesReceived = other.m_bytesReceived;
    }

    public static ClientStats diff(ClientStats newer, ClientStats older) {
        if ((newer.m_procName != older.m_procName) || (newer.m_connectionId != older.m_connectionId)) {
            throw new IllegalArgumentException("Can't diff these ClientStats instances.");
        }

        ClientStats retval = new ClientStats();
        retval.m_procName = older.m_procName;
        retval.m_connectionId = older.m_connectionId;
        retval.m_hostname = older.m_hostname;
        retval.m_port = older.m_port;
        retval.m_since = older.m_since;

        retval.m_invocationsCompleted = newer.m_invocationsCompleted - older.m_invocationsCompleted;
        retval.m_invocationAborts = newer.m_invocationAborts - older.m_invocationAborts;
        retval.m_invocationErrors = newer.m_invocationErrors - older.m_invocationErrors;

        retval.m_roundTripTime = newer.m_roundTripTime - older.m_roundTripTime;
        retval.m_clusterRoundTripTime = newer.m_clusterRoundTripTime - older.m_clusterRoundTripTime;

        retval.m_latencyBy1ms = LatencyBucketSet.diff(newer.m_latencyBy1ms, older.m_latencyBy1ms);
        retval.m_latencyBy10ms = LatencyBucketSet.diff(newer.m_latencyBy10ms, older.m_latencyBy10ms);
        retval.m_latencyBy100ms = LatencyBucketSet.diff(newer.m_latencyBy100ms, older.m_latencyBy100ms);

        retval.m_bytesSent = newer.m_bytesSent - older.m_bytesSent;
        retval.m_bytesReceived = newer.m_bytesReceived - older.m_bytesReceived;

        return retval;
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
        // non-destructive
        seed = (ClientStats) seed.clone();

        // add in all the other elements
        while (statsIter.hasNext()) {
            seed.add(statsIter.next());
        }
        return seed;
    }

    void add(ClientStats other) {
        if (m_procName.equals(other.m_procName) == false)  m_procName = "";
        if (m_connectionId != other.m_connectionId) m_connectionId = -1;
        if (m_hostname.equals(other.m_hostname) == false) m_hostname = "";
        if (m_port != other.m_port) m_port = -1;

        m_since = Math.min(other.m_since, m_since);

        m_invocationsCompleted = other.m_invocationsCompleted;
        m_invocationAborts = other.m_invocationAborts;
        m_invocationErrors = other.m_invocationErrors;

        m_roundTripTime += other.m_roundTripTime;
        m_clusterRoundTripTime += other.m_clusterRoundTripTime;

        m_latencyBy1ms.add(other.m_latencyBy1ms);
        m_latencyBy10ms.add(other.m_latencyBy10ms);
        m_latencyBy100ms.add(other.m_latencyBy100ms);

        m_bytesSent += other.m_bytesSent;
        m_bytesReceived += other.m_bytesReceived;
    }

    void update(int roundTripTime, int clusterRoundTripTime, boolean abort, boolean error) {
        m_invocationsCompleted++;
        if (abort) m_invocationAborts++;
        if (error) m_invocationErrors++;
        m_roundTripTime += roundTripTime;
        m_clusterRoundTripTime += clusterRoundTripTime;

        // calculate the latency buckets to increment and increment.
        m_latencyBy1ms.update(roundTripTime);
        m_latencyBy10ms.update(roundTripTime);
        m_latencyBy100ms.update(roundTripTime);
    }

    public String getProcedureName() {
        return m_procName;
    }

    public long getStartTimestamp() {
        return m_since;
    }

    public long getConnectionId() {
        return m_connectionId;
    }

    public String getHostname() {
        return m_hostname;
    }

    public int getPort() {
        return m_port;
    }

    public long getInvocationsCompleted() {
        return m_invocationsCompleted;
    }

    public long getInvocationAborts() {
        return m_invocationAborts;
    }

    public long getInvocationErrors() {
        return m_invocationErrors;
    }

    public long getAverageLatency() {
        if (m_invocationsCompleted == 0) return 0;
        return m_roundTripTime / m_invocationsCompleted;
    }

    public long getAverageInternalLatency() {
        if (m_invocationsCompleted == 0) return 0;
        return m_clusterRoundTripTime / m_invocationsCompleted;
    }

    public long[] getLatencyBucketsBy1ms() {
        return m_latencyBy1ms.buckets.clone();
    }

    public long[] getLatencyBucketsBy10ms() {
        return m_latencyBy10ms.buckets.clone();
    }

    public long[] getLatencyBucketsBy100ms() {
        return m_latencyBy100ms.buckets.clone();
    }

    public long getBytesWritten() {
        return m_bytesSent;
    }

    public long getBytesRead() {
        return m_bytesReceived;
    }

    public int kPercentileLatency(double percentile) {
        int kpl;

        kpl = m_latencyBy1ms.kPercentileLatency(percentile);
        if (kpl != Integer.MAX_VALUE) {
            return kpl;
        }

        kpl = m_latencyBy10ms.kPercentileLatency(percentile);
        if (kpl != Integer.MAX_VALUE) {
            return kpl;
        }

        kpl = m_latencyBy100ms.kPercentileLatency(percentile);
        if (kpl != Integer.MAX_VALUE) {
            return kpl;
        }

        return m_latencyBy100ms.msPerBucket * m_latencyBy100ms.numberOfBuckets * 2;
    }

    public long getTxnThroughput(long now) {
        if (m_invocationsCompleted == 0) return 0;
        if (now < m_since) {
            now = m_since + 1; // 1 ms duration is sorta cheatin'
        }
        long durationMs = now - m_since;
        return (long) (m_invocationsCompleted / (durationMs / 1000.0));
    }

    public long getIOWriteThroughput(long now) {
        long durationMs = now - m_since;
        return (long) (m_bytesSent / (durationMs / 1000.0));
    }

    public long getIOReadThroughput(long now) {
        long durationMs = now - m_since;
        return (long) (m_bytesReceived / (durationMs / 1000.0));
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("Since %s - Procedure: %s - ConnectionId: %d {\n",
                new Date(m_since).toString(), m_procName, m_connectionId));
        sb.append(String.format("    hostname: %s:%d\n",
                m_hostname, m_port));
        sb.append(String.format("    invocations completed/aborted/errors: %d/%d/%d\n",
                m_invocationsCompleted, m_invocationAborts, m_invocationErrors));
        if (m_invocationsCompleted > 0) {
            sb.append(String.format("    avg latency client/internal: %d/%d\n",
                    m_roundTripTime / m_invocationsCompleted, m_clusterRoundTripTime / m_invocationsCompleted));
            sb.append(m_latencyBy1ms).append("\n");
            sb.append(m_latencyBy10ms).append("\n");
            sb.append(m_latencyBy100ms).append("\n");
        }

        return sb.toString();
    }

    /* (non-Javadoc)
     * @see java.lang.Object#clone()
     */
    @Override
    protected Object clone() {
        return new ClientStats(this);
    }
}
