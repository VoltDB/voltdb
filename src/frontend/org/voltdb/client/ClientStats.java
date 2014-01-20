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

import java.util.Date;
import java.util.Iterator;

import org.voltdb.LatencyBucketSet;

/**
 * <p>Essentially a set of counters for a specific context with helper
 * methods. The context has a time window and can apply to all connections
 * and procedures, or a single of connections and/or procedure.</p>
 *
 * <p>The helper methods such as {@link #getTxnThroughput()} or
 * {@link #kPercentileLatency(double)} perform common operations
 * on the counters.</p>
 *
 * <p>This object is immutable outside of the package scope and does not
 * directly reference any internal data structures.</p>
 *
 * <p>See also {@link ClientStatsContext}.</p>
 */
public class ClientStats {
    String m_procName;
    long m_startTS; // java.util.Date compatible microseconds since epoch
    long m_endTS;

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

    /** The number of buckets tracking latency with 1ms granularity. */
    final public static int ONE_MS_BUCKET_COUNT = 50;
    /** The number of buckets tracking latency with 10ms granularity. */
    final public static int TEN_MS_BUCKET_COUNT = 20;
    /** The number of buckets tracking latency with 100ms granularity. */
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
        m_startTS = Long.MAX_VALUE;
        m_endTS = Long.MIN_VALUE;
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
        m_startTS = other.m_startTS;
        m_endTS = other.m_endTS;
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

    static ClientStats diff(ClientStats newer, ClientStats older) {
        if ((newer.m_procName != older.m_procName) || (newer.m_connectionId != older.m_connectionId)) {
            throw new IllegalArgumentException("Can't diff these ClientStats instances.");
        }

        ClientStats retval = new ClientStats();
        retval.m_procName = older.m_procName;
        retval.m_connectionId = older.m_connectionId;
        retval.m_hostname = older.m_hostname;
        retval.m_port = older.m_port;

        // the next two values are essentially useless after a diff, but will
        // be overwritten by ClientStatsContext
        retval.m_startTS = older.m_startTS;
        retval.m_endTS = newer.m_endTS;

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

        m_startTS = Math.min(other.m_startTS, m_startTS);
        m_endTS = Math.max(other.m_endTS, m_endTS);

        m_invocationsCompleted += other.m_invocationsCompleted;
        m_invocationAborts += other.m_invocationAborts;
        m_invocationErrors += other.m_invocationErrors;

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

    /**
     * Get the name of the procedure this statistics instance applies to.
     *
     * @return The name of the procedure or the empty string if this stats
     * instance covers more than one procedure.
     */
    public String getProcedureName() {
        return m_procName;
    }

    /**
     * Get the {@link Date}-compatible timestamp that describes the start of
     * the range of time this stats instance covers.
     *
     * @return A timestamp in milliseconds since the epoch.
     */
    public long getStartTimestamp() {
        return m_startTS;
    }

    /**
     * Get the {@link Date}-compatible timestamp that describes the end of
     * the range of time this stats instance covers.
     *
     * @return A timestamp in milliseconds since the epoch.
     */
    public long getEndTimestamp() {
        return m_endTS;
    }

    /**
     * Get the number of milliseconds this stats instance covers.
     *
     * @return The number of milliseconds this stats instance covers.
     */
    public long getDuration() {
        // this value should never be MIN_VALUE by the time a user can call this
        assert(m_endTS != Long.MIN_VALUE);
        return m_endTS - m_startTS;
    }

    /**
     * Get the id of the individual socket connection this statistics instance
     * applies to. Note that hostname and port combos might not be unique,
     * but connection ids will be.
     *
     * @return The id of the connection or -1 if this stats instance covers more
     * than one connection.
     */
    public long getConnectionId() {
        return m_connectionId;
    }

    /**
     * The hostname or IP (as string) of the connection this stats instance
     * covers.
     *
     * @return The hostname or ip as string, or the empty string if this stats
     * instance covers more than one connection.
     */
    public String getHostname() {
        return m_hostname;
    }

    /**
     * The port number of the connection this stats instance covers.
     *
     * @return The port number, or -1 if this stats instance covers more than
     * one connection.
     */
    public int getPort() {
        return m_port;
    }

    /**
     * Get the number of transactions acknowledged by the VoltDB server(s)
     * during the time period covered by this stats instance.
     *
     * @return The number of transactions completed.
     */
    public long getInvocationsCompleted() {
        return m_invocationsCompleted;
    }

    /**
     * Get the number of transactions aborted by the VoltDB server(s)
     * during the time period covered by this stats instance.
     *
     * @return The number of transactions aborted.
     */
    public long getInvocationAborts() {
        return m_invocationAborts;
    }

    /**
     * Get the number of transactions failed by the VoltDB server(s)
     * during the time period covered by this stats instance.
     *
     * @return The number of transactions that failed.
     */
    public long getInvocationErrors() {
        return m_invocationErrors;
    }

    /**
     * Get the average latency in milliseconds for the time period
     * covered by this stats instance. This is computed by summing the client-measured
     * round trip times of all transactions and dividing by the competed
     * invocation count.
     *
     * @return Average latency in milliseconds.
     */
    public double getAverageLatency() {
        if (m_invocationsCompleted == 0) return 0;
        return (double)m_roundTripTime / (double)m_invocationsCompleted;
    }

    /**
     * <p>Get the server-side average latency in milliseconds for the time period
     * covered by this stats instance. This is computed by summing the server-reported
     * latency times of all transactions and dividing by the competed invocation count.</p>
     *
     * <p>The server reported latency number measures the time from when a transaction
     * is accepted from the socket to when the response is written back. It will be higher
     * for multi-node clusters, for clusters with too much load, or for clusters with longer
     * running transactions.</p>
     *
     * @return Average latency in milliseconds.
     */
    public double getAverageInternalLatency() {
        if (m_invocationsCompleted == 0) return 0;
        return (double)m_clusterRoundTripTime / (double)m_invocationsCompleted;
    }

    /**
     * <p>Get the raw buckets used for latency tracking in 1ms increments. For example, if
     * a transaction returns in 3.2ms, then the array at index 3 will be incremented by
     * one. It can be thought of as a histogram of latencies. It has
     * {@link #ONE_MS_BUCKET_COUNT} buckets, for a range of
     * <code>ONE_MS_BUCKET_COUNT x 1ms</code></p>
     *
     * <p>This raw data, along with other bucket sets of different granularity,  is used to
     * support the {@link #kPercentileLatency(double)} method. This returns a copy of the
     * internal array so it is threadsafe and mutable if you wish. Note that the buckets
     *
     * @return An array containing counts for different latency values.
     */
    public long[] getLatencyBucketsBy1ms() {
        return m_latencyBy1ms.buckets.clone();
    }

    /**
     * <p>Get the raw buckets used for latency tracking in 10ms increments. For example, if
     * a transaction returns in 42ms, then the array at index 4 will be incremented by
     * one. It can be thought of as a histogram of latencies. It has
     * {@link #TEN_MS_BUCKET_COUNT} buckets, for a range of
     * <code>TEN_MS_BUCKET_COUNT x 10ms</code>.</p>
     *
     * <p>This raw data, along with other bucket sets of different granularity,  is used to
     * support the {@link #kPercentileLatency(double)} method. This returns a copy of the
     * internal array so it is threadsafe and mutable if you wish. Note that the buckets
     *
     * @return An array containing counts for different latency values.
     */
    public long[] getLatencyBucketsBy10ms() {
        return m_latencyBy10ms.buckets.clone();
    }

    /**
     * <p>Get the raw buckets used for latency tracking in 1ms increments. For example, if
     * a transaction returns in 3.2ms, then the array at index 3 will be incremented by
     * one. It can be thought of as a histogram of latencies. It has
     * {@link #HUNDRED_MS_BUCKET_COUNT} buckets, for a range of
     * <code>HUNDRED_MS_BUCKET_COUNT x 100ms</code>.</p>
     *
     * <p>This raw data, along with other bucket sets of different granularity,  is used to
     * support the {@link #kPercentileLatency(double)} method. This returns a copy of the
     * internal array so it is threadsafe and mutable if you wish. Note that the buckets
     *
     * @return An array containing counts for different latency values.
     */
    public long[] getLatencyBucketsBy100ms() {
        return m_latencyBy100ms.buckets.clone();
    }

    /**
     * Return the number of bytes written over the network during the time period
     * covered by this stats instance. This can be specific to a connection or global,
     * but is not recorded for per-procedure statistics.
     *
     * @return The number of bytes written or 0 for per-procedure statistics.
     */
    public long getBytesWritten() {
        return m_bytesSent;
    }

    /**
     * Return the number of bytes read from the network during the time period
     * covered by this stats instance. This can be specific to a connection or global,
     * but is not recorded for per-procedure statistics.
     *
     * @return The number of bytes read or 0 for per-procedure statistics.
     */
    public long getBytesRead() {
        return m_bytesReceived;
    }

    /**
     * <p>Using the latency bucketing statistics gathered by the client, estimate
     * the k-percentile latency value for the time period covered by this stats
     * instance.</p>
     *
     * <p>For example, k=.5 returns an estimate of the median. k=0 returns the
     * minimum. k=1.0 returns the maximum.</p>
     *
     * <p>The accuracy is limited by the precision of the buckets, and will have
     * higher margin of error as courser sets of buckets are used. Small numbers
     * of transactions in the period will also increase error. Finally, note that
     * latency isn't tracked for transactions are outside values tracked by the
     * largest set of buckets. If k=X implies latency greater than
     * <code>{@link #HUNDRED_MS_BUCKET_COUNT} * 100</code>, then it will return
     * a number larger than <code>{@link #HUNDRED_MS_BUCKET_COUNT} * 100</code>,
     * but nothing more is promised.</p>
     *
     * @param percentile A floating point number between 0.0 and 1.0.
     * @return An estimate of k-percentile latency in whole milliseconds.
     */
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

    /**
     * Generate a human-readable report of latencies in the form of a histogram.
     *
     * @return String containing human-readable report.
     */
    public String latencyHistoReport() {
        StringBuilder sb = new StringBuilder();

        // for now, I believe 3 digit accuracy is enough
        int upper = kPercentileLatency(0.99999);
        int high = kPercentileLatency(0.99);

        if(high <= m_latencyBy1ms.numberOfBuckets * m_latencyBy1ms.msPerBucket){
            if(upper <= m_latencyBy1ms.numberOfBuckets * m_latencyBy1ms.msPerBucket) {
                return m_latencyBy1ms.latencyHistoReport(upper);
            } else {
                // 99% are within the range, ignore 1% outliners for more accurate result
                return m_latencyBy1ms.latencyHistoReport(high);
            }
        } else if(upper <= m_latencyBy10ms.numberOfBuckets * m_latencyBy10ms.msPerBucket) {
           return m_latencyBy10ms.latencyHistoReport(upper);
        } else if(upper <= m_latencyBy100ms.numberOfBuckets * m_latencyBy100ms.msPerBucket) {
            return m_latencyBy100ms.latencyHistoReport(upper);
        } else {
            // rare case; add one more bin to calculate percentile for txns with latency over 1000ms
            sb.append(m_latencyBy100ms.latencyHistoReport(m_latencyBy100ms.msPerBucket * m_latencyBy100ms.numberOfBuckets));
            sb.append(String.format(">%1-10sms:", m_latencyBy100ms.msPerBucket * m_latencyBy100ms.numberOfBuckets));
            int height = (int)Math.ceil(m_latencyBy100ms.unaccountedTxns * m_latencyBy100ms.maxBinHeight / m_latencyBy100ms.totalTxns);
            for(int i = 0; i < height; i++) {
                sb.append("|");
            }
            sb.append(String.format("]%7.3f%%\n", ((double)m_latencyBy100ms.unaccountedTxns / (double)m_latencyBy100ms.totalTxns * 100)));
            return sb.toString();
        }
    }

    /**
     * <p>Return an average throughput of transactions acknowledged per
     * second for the duration covered by this stats instance.</p>
     *
     * <p>Essentially <code>{@link #getInvocationsCompleted()} divided by
     * ({@link #getStartTimestamp()} - {@link #getEndTimestamp()} / 1000.0)</code>,
     * but with additional safety checks.</p>
     *
     * @return Throughput in transactions acknowledged per second.
     */
    public long getTxnThroughput() {
        assert(m_startTS != Long.MAX_VALUE);
        assert(m_endTS != Long.MIN_VALUE);

        if (m_invocationsCompleted == 0) return 0;
        if (m_endTS < m_startTS) {
            m_endTS = m_startTS + 1; // 1 ms duration is sorta cheatin'
        }
        long durationMs = m_endTS - m_startTS;
        return (long) (m_invocationsCompleted / (durationMs / 1000.0));
    }

    /**
     * <p>Return an average throughput of bytes sent per second over the
     * network for the duration covered by this stats instance.</p>
     *
     * <p>Essentially <code>{@link #getBytesWritten()} divided by
     * ({@link #getStartTimestamp()} - {@link #getEndTimestamp()} / 1000.0)</code>,
     * but with additional safety checks.</p>
     *
     * @return Throughput in bytes sent per second.
     */
    public long getIOWriteThroughput() {
        assert(m_startTS != Long.MAX_VALUE);
        assert(m_endTS != Long.MIN_VALUE);

        if (m_bytesSent == 0) return 0;
        if (m_endTS < m_startTS) {
            m_endTS = m_startTS + 1; // 1 ms duration is sorta cheatin'
        }

        long durationMs = m_endTS - m_startTS;
        return (long) (m_bytesSent / (durationMs / 1000.0));
    }

    /**
     * <p>Return an average throughput of bytes read per second from the
     * network for the duration covered by this stats instance.</p>
     *
     * <p>Essentially <code>{@link #getBytesRead()} divided by
     * ({@link #getStartTimestamp()} - {@link #getEndTimestamp()} / 1000.0)</code>,
     * but with additional safety checks.</p>
     *
     * @return Throughput in bytes read per second.
     */
    public long getIOReadThroughput() {
        assert(m_startTS != Long.MAX_VALUE);
        assert(m_endTS != Long.MIN_VALUE);

        if (m_bytesReceived == 0) return 0;
        if (m_endTS < m_startTS) {
            m_endTS = m_startTS + 1; // 1 ms duration is sorta cheatin'
        }

        long durationMs = m_endTS - m_startTS;
        return (long) (m_bytesReceived / (durationMs / 1000.0));
    }

    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("Start %s - End %s - Procedure: %s - ConnectionId: %d {\n",
                new Date(m_startTS).toString(), new Date(m_endTS).toString(), m_procName, m_connectionId));
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
