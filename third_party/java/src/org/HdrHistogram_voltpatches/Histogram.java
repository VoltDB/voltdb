/**
 * Histogram.java
 * Written by Gil Tene of Azul Systems, and released to the public domain,
 * as explained at http://creativecommons.org/publicdomain/zero/1.0/
 *
 * @author Gil Tene
 */

package org.HdrHistogram_voltpatches;

import java.io.IOException;
import java.io.ObjectInputStream;

/**
 * <h3>A High Dynamic Range (HDR) Histogram</h3>
 * <p>
 * Histogram supports the recording and analyzing sampled data value counts across a configurable integer value
 * range with configurable value precision within the range. Value precision is expressed as the number of significant
 * digits in the value recording, and provides control over value quantization behavior across the value range and the
 * subsequent value resolution at any given level.
 * <p>
 * For example, a Histogram could be configured to track the counts of observed integer values between 0 and
 * 3,600,000,000 while maintaining a value precision of 3 significant digits across that range. Value quantization
 * within the range will thus be no larger than 1/1,000th (or 0.1%) of any value. This example Histogram could
 * be used to track and analyze the counts of observed response times ranging between 1 microsecond and 1 hour
 * in magnitude, while maintaining a value resolution of 1 microsecond up to 1 millisecond, a resolution of
 * 1 millisecond (or better) up to one second, and a resolution of 1 second (or better) up to 1,000 seconds. At it's
 * maximum tracked value (1 hour), it would still maintain a resolution of 3.6 seconds (or better).
 * <p>
 * Histogram tracks value counts in <b><code>long</code></b> fields. Smaller field types are available in the
 * {@link org.HdrHistogram_voltpatches.IntHistogram} and {@link org.HdrHistogram_voltpatches.ShortHistogram} implementations of
 * {@link org.HdrHistogram_voltpatches.AbstractHistogram}.
 * <p>
 * See package description for {@link org.HdrHistogram_voltpatches} for details.
 */

public class Histogram extends AbstractHistogram {
    long totalCount;
    final long[] counts;

    @Override
    long getCountAtIndex(final int index) {
        return counts[index];
    }

    @Override
    void incrementCountAtIndex(final int index) {
        counts[index]++;
    }

    @Override
    void addToCountAtIndex(final int index, final long value) {
        counts[index] += value;
    }

    @Override
    void clearCounts() {
        java.util.Arrays.fill(counts, 0);
        totalCount = 0;
    }

    /**
     * @inheritDoc
     */
    @Override
    public Histogram copy() {
      Histogram copy = new Histogram(lowestTrackableValue, highestTrackableValue, numberOfSignificantValueDigits);
      copy.add(this);
      return copy;
    }

    /**
     * @inheritDoc
     */
    @Override
    public Histogram copyCorrectedForCoordinatedOmission(final long expectedIntervalBetweenValueSamples) {
        Histogram toHistogram = new Histogram(lowestTrackableValue, highestTrackableValue, numberOfSignificantValueDigits);
        toHistogram.addWhileCorrectingForCoordinatedOmission(this, expectedIntervalBetweenValueSamples);
        return toHistogram;
    }

    @Override
    long getTotalCount() {
        return totalCount;
    }

    @Override
    void setTotalCount(final long totalCount) {
        this.totalCount = totalCount;
    }

    @Override
    void incrementTotalCount() {
        totalCount++;
    }

    @Override
    void addToTotalCount(final long value) {
        totalCount += value;
    }

    /**
     * Provide a (conservatively high) estimate of the Histogram's total footprint in bytes
     *
     * @return a (conservatively high) estimate of the Histogram's total footprint in bytes
     */
    @Override
    public int getEstimatedFootprintInBytes() {
        return (512 + (8 * counts.length));
    }


    /**
     * Construct a Histogram given the Highest value to be tracked and a number of significant decimal digits. The
     * histogram will be constructed to implicitly track (distinguish from 0) values as low as 1.
     *
     * @param highestTrackableValue The highest value to be tracked by the histogram. Must be a positive
     *                              integer that is >= 2.
     * @param numberOfSignificantValueDigits The number of significant decimal digits to which the histogram will
     *                                       maintain value resolution and separation. Must be a non-negative
     *                                       integer between 0 and 5.
     */
    public Histogram(final long highestTrackableValue, final int numberOfSignificantValueDigits) {
        this(1, highestTrackableValue, numberOfSignificantValueDigits);
    }

    /**
     * Construct a Histogram given the Lowest and Highest values to be tracked and a number of significant
     * decimal digits. Providing a lowestTrackableValue is useful is situations where the units used
     * for the histogram's values are much smaller that the minimal accuracy required. E.g. when tracking
     * time values stated in nanosecond units, where the minimal accuracy required is a microsecond, the
     * proper value for lowestTrackableValue would be 1000.
     *
     * @param lowestTrackableValue The lowest value that can be tracked (distinguished from 0) by the histogram.
     *                             Must be a positive integer that is >= 1. May be internally rounded down to nearest
     *                             power of 2.
     * @param highestTrackableValue The highest value to be tracked by the histogram. Must be a positive
     *                              integer that is >= (2 * lowestTrackableValue).
     * @param numberOfSignificantValueDigits The number of significant decimal digits to which the histogram will
     *                                       maintain value resolution and separation. Must be a non-negative
     *                                       integer between 0 and 5.
     */
    public Histogram(final long lowestTrackableValue, final long highestTrackableValue, final int numberOfSignificantValueDigits) {
        super(lowestTrackableValue, highestTrackableValue, numberOfSignificantValueDigits);
        counts = new long[countsArrayLength];
    }

    private void readObject(final ObjectInputStream o)
            throws IOException, ClassNotFoundException {
        o.defaultReadObject();
    }

    public static Histogram diff(Histogram newer, Histogram older) {
        Histogram h = new Histogram(newer.getLowestTrackableValue(), newer.getHighestTrackableValue(), newer.getNumberOfSignificantValueDigits());
        h.totalCount = newer.totalCount - older.totalCount;
        for (int ii = 0; ii < h.countsArrayLength; ii++) {
            h.counts[ii] = newer.counts[ii] - older.counts[ii];
        }
        h.reestablishTotalCount();
        return h;
    }
}