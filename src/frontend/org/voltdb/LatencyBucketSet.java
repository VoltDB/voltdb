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

package org.voltdb;

public class LatencyBucketSet {
    /** */
    public final int numberOfBuckets;
    /** */
    public final int msPerBucket;
    /** */
    public long unaccountedTxns = 0;
    /** */
    public long totalTxns = 0;
    /** */
    public long buckets[];

    public int maxNumberOfBins = 10;
    public long maxBinHeight = 70;

    public LatencyBucketSet(int msPerBucket, int numberOfBuckets) {
        this.msPerBucket = msPerBucket;
        this.numberOfBuckets = numberOfBuckets;
        buckets = new long[numberOfBuckets];
    }

    public void update(int newLatencyValue) {
        ++totalTxns;

        // set floor for latency
        if (newLatencyValue < 0) newLatencyValue = 0;

        // update the bucket
        int bucketOffset = newLatencyValue / msPerBucket;
        if (bucketOffset >= numberOfBuckets) {
            ++unaccountedTxns;
        }
        else {
            ++buckets[bucketOffset];
        }
    }

    /**
     *
     * @param percentile A number in [0.0, 1.0).
     * @return An estimate of k-percentile latency in ms if possible, and
     * {@link Integer#MAX_VALUE} if the k-th transaction is not contained
     * in a bucket.
     */
    public int kPercentileLatency(double percentile) {
        if ((percentile > 1.0) || (percentile < 0.0)) {
            throw new IllegalArgumentException(
                    "KPercentileLatency accepts values greater or equal to 0.0 " +
                    "and less than or equal to 1.0");
        }

        // 0 for no txns?
        if (totalTxns == 0) return 0;

        // find the number of calls with less than percentile latency
        long k = (long) (totalTxns * percentile);
        // ensure k=0 gives min latency
        if (k == 0) ++k;
        if (k > totalTxns) k = totalTxns; // this shouldn't happen but FP math is iffy

        // if 0 or outside the range of buckets, return MAX_VALUE
        if ((totalTxns == 0) || (k > (totalTxns - unaccountedTxns))) {
            return Integer.MAX_VALUE;
        }

        long sum = 0;
        for (int i = 0; i < numberOfBuckets; i++) {
            sum += buckets[i];
            if (sum >= k) {
                // return the midpoint of the winning bucket's range
                return i * msPerBucket + (int) Math.ceil(msPerBucket / 2.0);
            }
        }

        // should not ever get here as the range check should ensure
        // the for loop above never finishes
        assert(false);
        return -1;
    }

    public static LatencyBucketSet merge(LatencyBucketSet lbs1, LatencyBucketSet lbs2) {
        LatencyBucketSet retval = (LatencyBucketSet) lbs1.clone();
        retval.add(lbs2);
        return retval;
    }

    public void add(LatencyBucketSet other) {
        if ((msPerBucket != other.msPerBucket) || (numberOfBuckets != other.numberOfBuckets)) {
            throw new IllegalArgumentException(
                    "Adding LatencyBucketSet instances requires both cover the same range.");
        }

        for (int i = 0; i < numberOfBuckets; ++i) {
            buckets[i] += other.buckets[i];
        }
        totalTxns += other.totalTxns;
        unaccountedTxns += other.unaccountedTxns;
    }

    public static LatencyBucketSet diff(LatencyBucketSet newer, LatencyBucketSet older) {
        LatencyBucketSet retval = (LatencyBucketSet) newer.clone();
        for (int i = 0; i < retval.numberOfBuckets; i++) {
            retval.buckets[i] -= older.buckets[i];
        }
        retval.totalTxns -= older.totalTxns;
        retval.unaccountedTxns -= older.unaccountedTxns;
        return retval;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#clone()
     */
    @Override
    public Object clone() {
        LatencyBucketSet retval = new LatencyBucketSet(msPerBucket, numberOfBuckets);
        retval.buckets = buckets.clone();
        retval.totalTxns = totalTxns;
        retval.unaccountedTxns = unaccountedTxns;
        return retval;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("0-%dms by %dms: [",
                msPerBucket * numberOfBuckets, msPerBucket));
        for (int i = 0; i < numberOfBuckets; i++) {
            sb.append(buckets[i]);
            if (i == (numberOfBuckets - 1)) {
                sb.append("]");
            }
            else {
                sb.append(", ");
            }
        }
        return sb.toString();
    }

    /**
     * Generate histo report with current latency granularity (this latency bucket set)
     * For all executed txns, if
     *         - kPercentileLatency(0.99) <= 50ms: use 1ms latency bucket set
     *         - kPercentileLatency(0.999) <= 200ms: use 10ms latency bucket set
     *         - otherwise: use 100ms latency bucket set [ + an additional bin]
     * The measurement unit here is "bucket" (N ms), so:
     *     total range (# of units) of the histogram <=> upper / msPerBucket
     *     # of bins <=> (# of units > max # of units) ? resize() : (# of units)
     *     width of bin <=> (# of units) / (# of bins)
     * For each bin: calculate # of txns falling in the buckets belonging to it
     * @param upper <= msPerBucket * numberOfBucket
     * @return
     */
    public String latencyHistoReport(int upper) {
        StringBuilder sb = new StringBuilder();

        // Try to use finer granularity here, resize when really needed
        // 100ms latency bucket set: return 1
        // 10ms latency bucket set: return 1 or 2 (if upper > 120ms)
        // 1ms latency bucket set: return 1 (if upper <= 12ms) or (upper / 10)
        int numberOfBucketsPerBin = (upper <= maxNumberOfBins * msPerBucket * 1.2) ? 1 :
                                        (int)Math.ceil((double) upper / (double) maxNumberOfBins / (double) msPerBucket);
        int binWidth = numberOfBucketsPerBin * msPerBucket;
        int numberOfBins = (int)Math.ceil(upper / binWidth);

        for(int i = 0; i < numberOfBins; i++) {
            sb.append(String.format("%1$-4s - %2$-4sms: [", i * numberOfBucketsPerBin * msPerBucket,
                        (i * numberOfBucketsPerBin + numberOfBucketsPerBin) * msPerBucket));
            long txnsInCurrentBin = 0;
            for (int j = 0; j < numberOfBucketsPerBin; j++) {
                txnsInCurrentBin += buckets[i * numberOfBucketsPerBin + j];
            }
            int binHeight = (int)Math.ceil(txnsInCurrentBin * maxBinHeight / totalTxns);
            for (int j = 0; j < binHeight; j++) {
                sb.append("|");
            }
            sb.append(String.format("]%7.3f%%\n", (double)txnsInCurrentBin / (double)totalTxns * 100));
        }
        return sb.toString();
    }
}
