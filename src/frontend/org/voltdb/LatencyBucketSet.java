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

    public LatencyBucketSet(int msPerBucket, int numberOfBuckets) {
        this.msPerBucket = msPerBucket;
        this.numberOfBuckets = numberOfBuckets;
        buckets = new long[numberOfBuckets];
    }

    public void update(int newLatencyValue) {
        ++totalTxns;
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
}
