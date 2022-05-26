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

package org.voltdb.client;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import org.HdrHistogram_voltpatches.Histogram;

import com.google_voltpatches.common.base.Charsets;
import com.google_voltpatches.common.base.Throwables;

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
    long m_startTS; // java.util.Date compatible milliseconds since epoch
    long m_endTS;

    long m_connectionId;
    String m_hostname;
    int m_port;

    long m_invocationsCompleted;
    long m_invocationAborts;
    long m_invocationErrors;
    long m_invocationTimeouts;

    // cumulative latency measured by client, used to calculate avg. lat.
    long m_roundTripTimeNanos;
    // cumulative latency measured by the cluster, used to calculate avg lat.
    long m_clusterRoundTripTime; // milliseconds

    /** The number of buckets tracking latency with 1ms granularity. */
    final public static int ONE_MS_BUCKET_COUNT = 50;
    /** The number of buckets tracking latency with 10ms granularity. */
    final public static int TEN_MS_BUCKET_COUNT = 20;
    /** The number of buckets tracking latency with 100ms granularity. */
    final public static int HUNDRED_MS_BUCKET_COUNT = 10;

    Histogram m_latencyHistogram;

    long m_bytesSent;
    long m_bytesReceived;

    private static final long LOWEST_TRACKABLE = 50;
    private static final long HIGHEST_TRACKABLE = 10L * (1000L * 1000L);
    private static final int SIGNIFICANT_VALUE_DIGITS = 2;

    /*
     * Get a that tracks from 1 microsecond to 10 seconds with
     * 2 significant value digits
     */
    public static Histogram constructHistogram() {
        return new Histogram( LOWEST_TRACKABLE, HIGHEST_TRACKABLE, SIGNIFICANT_VALUE_DIGITS);
    }

    ClientStats() {
        m_procName = "";
        m_connectionId = -1;
        m_hostname = "";
        m_port = -1;
        m_startTS = Long.MAX_VALUE;
        m_endTS = Long.MIN_VALUE;
        m_invocationsCompleted = m_invocationAborts = m_invocationErrors = 0;
        m_roundTripTimeNanos = m_clusterRoundTripTime = 0;
        m_bytesSent = m_bytesReceived = 0;
        m_latencyHistogram = constructHistogram();
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
        m_invocationTimeouts = other.m_invocationTimeouts;
        m_roundTripTimeNanos = other.m_roundTripTimeNanos;
        m_clusterRoundTripTime = other.m_clusterRoundTripTime;
        m_latencyHistogram = other.m_latencyHistogram.copy();
        m_latencyHistogram.reestablishTotalCount();
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
        retval.m_invocationTimeouts = newer.m_invocationTimeouts - older.m_invocationTimeouts;

        retval.m_roundTripTimeNanos = newer.m_roundTripTimeNanos - older.m_roundTripTimeNanos;
        retval.m_clusterRoundTripTime = newer.m_clusterRoundTripTime - older.m_clusterRoundTripTime;

        retval.m_latencyHistogram = Histogram.diff(newer.m_latencyHistogram, older.m_latencyHistogram);

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
        m_invocationTimeouts += other.m_invocationTimeouts;

        m_roundTripTimeNanos += other.m_roundTripTimeNanos;
        m_clusterRoundTripTime += other.m_clusterRoundTripTime;

        m_latencyHistogram.add(other.m_latencyHistogram);
        m_latencyHistogram.reestablishTotalCount();

        m_bytesSent += other.m_bytesSent;
        m_bytesReceived += other.m_bytesReceived;
    }

    void update(long roundTripTimeNanos, int clusterRoundTripTime, boolean abort, boolean error, boolean timeout) {
        m_invocationsCompleted++;
        if (abort) m_invocationAborts++;
        if (error) m_invocationErrors++;
        if (timeout) m_invocationTimeouts++;
        m_roundTripTimeNanos += roundTripTimeNanos;
        m_clusterRoundTripTime += clusterRoundTripTime;

        //Round up to 50 microseconds. Average is still accurate and it doesn't change the percentile distribution
        //above 50 micros
        final long roundTripMicros = Math.max(LOWEST_TRACKABLE, TimeUnit.NANOSECONDS.toMicros(roundTripTimeNanos));
        if (roundTripMicros > HIGHEST_TRACKABLE) {
            m_latencyHistogram.recordValue(roundTripMicros % HIGHEST_TRACKABLE);
            int count = (int)(roundTripMicros / HIGHEST_TRACKABLE);
            for (int ii = 0; ii < count; ii++) {
                m_latencyHistogram.recordValue(HIGHEST_TRACKABLE);
            }
        } else {
            m_latencyHistogram.recordValue(roundTripMicros);
        }
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
     * Get the number of transactions timed out before being sent to or responded by VoltDB server(s)
     * during the time period covered by this stats instance.
     *
     * @return The number of transactions that failed.
     */
    public long getInvocationTimeouts() {
        return m_invocationTimeouts;
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
        return (m_roundTripTimeNanos / (double)m_invocationsCompleted) / 1000000.0D;
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
        final long buckets[] = new long[ONE_MS_BUCKET_COUNT];
        for (int ii = 0; ii < ONE_MS_BUCKET_COUNT; ii++) {
            buckets[ii] = m_latencyHistogram.getCountBetweenValues(ii * 1000L, (ii + 1) * 1000L);
        }
        return buckets;
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
        final long buckets[] = new long[TEN_MS_BUCKET_COUNT];
        for (int ii = 0; ii < TEN_MS_BUCKET_COUNT; ii++) {
            buckets[ii] = m_latencyHistogram.getCountBetweenValues(ii * 10000L, (ii + 1) * 10000L);
        }
        return buckets;
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
        final long buckets[] = new long[HUNDRED_MS_BUCKET_COUNT];
        for (int ii = 0; ii < HUNDRED_MS_BUCKET_COUNT; ii++) {
            buckets[ii] = m_latencyHistogram.getCountBetweenValues(ii * 100000L, (ii + 1) * 100000L);
        }
        return buckets;
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
     * <p>Latencies longer than the highest trackable value (10 seconds) will be
     * reported as multiple entries at the highest trackable value</p>
     *
     * @param percentile A floating point number between 0.0 and 1.0.
     * @return An estimate of k-percentile latency in whole milliseconds.
     */
    public int kPercentileLatency(double percentile) {
        if (m_latencyHistogram.getTotalCount() == 0) return 0;
        percentile = Math.max(0.0D, percentile);
        //Convert from micros to millis for return value, round to nearest integer
        return (int) (Math.round(m_latencyHistogram.getValueAtPercentile(percentile * 100.0D)) / 1000.0);
    }

    /**
     * <p>Using the latency bucketing statistics gathered by the client, estimate
     * the k-percentile latency value for the time period covered by this stats
     * instance.</p>
     *
     * <p>For example, k=.5 returns an estimate of the median. k=0 returns the
     * minimum. k=1.0 returns the maximum.</p>
     *
     * <p>Latencies longer than the highest trackable value (10 seconds) will be
     * reported as multiple entries at the highest trackable value</p>
     *
     * @param percentile A floating point number between 0.0 and 1.0.
     * @return An estimate of k-percentile latency in whole milliseconds.
     */
    public double kPercentileLatencyAsDouble(double percentile) {
        if (m_latencyHistogram.getTotalCount() == 0) return 0.0;
        percentile = Math.max(0.0D, percentile);
        //Convert from micros to millis for return value, enjoy having precision
        return m_latencyHistogram.getValueAtPercentile(percentile * 100.0D) / 1000.0;
    }

    /**
     * Generate a human-readable report of latencies in the form of a histogram. Latency is
     * in milliseconds
     *
     * @return String containing human-readable report.
     */
    public String latencyHistoReport() {
        ByteArrayOutputStream baos= new ByteArrayOutputStream();
        PrintStream pw = null;
        try {
            pw = new PrintStream(baos, false, Charsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            Throwables.propagate(e);
        }

        //Get a latency report in milliseconds
        m_latencyHistogram.outputPercentileDistributionVolt(pw, 1, 1000.0);

        return new String(baos.toByteArray(), Charsets.UTF_8);
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
        sb.append(String.format("    invocations completed/aborted/errors/timeouts: %d/%d/%d/%d\n",
                m_invocationsCompleted, m_invocationAborts, m_invocationErrors, m_invocationTimeouts));
        if (m_invocationsCompleted > 0) {
            sb.append(String.format("    avg latency client/internal: %.2f/%d\n",
                    (m_roundTripTimeNanos / (double)m_invocationsCompleted) / 1000000.0, m_clusterRoundTripTime / m_invocationsCompleted));
            sb.append(latencyHistoReport()).append("\n");
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
