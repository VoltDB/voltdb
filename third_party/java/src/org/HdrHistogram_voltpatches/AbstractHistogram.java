/**
 * AbstractHistogram.java
 * Written by Gil Tene of Azul Systems, and released to the public domain,
 * as explained at http://creativecommons.org/publicdomain/zero/1.0/
 *
 * @author Gil Tene
 */

package org.HdrHistogram_voltpatches;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.atomic.AtomicLong;

import org.voltcore.utils.CompressionStrategy;

/**
 * This non-public AbstractHistogramBase super-class separation is meant to bunch "cold" fields
 * separately from "hot" fields, in an attempt to force the JVM to place the (hot) fields
 * commonly used in the value recording code paths close together.
 * Subclass boundaries tend to be strongly control memory layout decisions in most practical
 * JVM implementations, making this an effective method for control filed grouping layout.
 */

abstract class AbstractHistogramBase {
    static AtomicLong constructionIdentityCount = new AtomicLong(0);

    // "Cold" accessed fields. Not used in the recording code path:
    long identityCount;

    long highestTrackableValue;
    long lowestTrackableValue;
    int numberOfSignificantValueDigits;

    int bucketCount;
    int subBucketCount;
    int countsArrayLength;

    HistogramData histogramData;
}

/**
 * <h3>A High Dynamic Range (HDR) Histogram</h3>
 * <p>
 * AbstractHistogram supports the recording and analyzing sampled data value counts across a configurable integer value
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
 * See package description for {@link org.HdrHistogram_voltpatches} for details.
 *
 */

public abstract class AbstractHistogram extends AbstractHistogramBase implements Serializable {
    // "Hot" accessed fields (used in the the value recording code path) are bunched here, such
    // that they will have a good chance of ending up in the same cache line as the totalCounts and
    // counts array reference fields that subclass implementations will typically add.
    int subBucketHalfCountMagnitude;
    int unitMagnitude;
    int subBucketHalfCount;
    long subBucketMask;
    // Sub-classes will typically add a totalCount field and a counts array field, which will likely be laid out
    // right around here due to the subclass layout rules in most practical JVM implementations.

    // Abstract, counts-type dependent methods to be provided by subclass implementations:

    abstract long getCountAtIndex(int index);

    abstract void incrementCountAtIndex(int index);

    abstract void addToCountAtIndex(int index, long value);

    abstract long getTotalCount();

    abstract void setTotalCount(long totalCount);

    abstract void incrementTotalCount();

    abstract void addToTotalCount(long value);

    abstract void clearCounts();

    /**
     * Create a copy of this histogram, complete with data and everything.
     *
     * @return A distinct copy of this histogram.
     */
    abstract public AbstractHistogram copy();

    /**
     * Get a copy of this histogram, corrected for coordinated omission.
     * <p>
     * To compensate for the loss of sampled values when a recorded value is larger than the expected
     * interval between value samples, the new histogram will include an auto-generated additional series of
     * decreasingly-smaller (down to the expectedIntervalBetweenValueSamples) value records for each count found
     * in the current histogram that is larger than the expectedIntervalBetweenValueSamples.
     *
     * Note: This is a post-correction method, as opposed to the at-recording correction method provided
     * by {@link #recordValueWithExpectedInterval(long, long) recordValueWithExpectedInterval}. The two
     * methods are mutually exclusive, and only one of the two should be be used on a given data set to correct
     * for the same coordinated omission issue.
     * by
     * <p>
     * See notes in the description of the Histogram calls for an illustration of why this corrective behavior is
     * important.
     *
     * @param expectedIntervalBetweenValueSamples If expectedIntervalBetweenValueSamples is larger than 0, add
     *                                           auto-generated value records as appropriate if value is larger
     *                                           than expectedIntervalBetweenValueSamples
     * @throws ArrayIndexOutOfBoundsException
     */
    abstract public AbstractHistogram copyCorrectedForCoordinatedOmission(final long expectedIntervalBetweenValueSamples);

    /**
     * Provide a (conservatively high) estimate of the Histogram's total footprint in bytes
     *
     * @return a (conservatively high) estimate of the Histogram's total footprint in bytes
     */
    abstract public int getEstimatedFootprintInBytes();

    /**
     * Copy this histogram into the target histogram, overwriting it's contents.
     *
     * @param targetHistogram
     */
    public void copyInto(AbstractHistogram targetHistogram) {
        targetHistogram.reset();
        targetHistogram.add(this);
    }

    /**
     * Copy this histogram, corrected for coordinated omission, into the target histogram, overwriting it's contents.
     * (see {@link #copyCorrectedForCoordinatedOmission} for more detailed explanation about how correction is applied)
     *
     * @param targetHistogram
     * @param expectedIntervalBetweenValueSamples
     */
    public void copyIntoCorrectedForCoordinatedOmission(AbstractHistogram targetHistogram, final long expectedIntervalBetweenValueSamples) {
        targetHistogram.reset();
        targetHistogram.addWhileCorrectingForCoordinatedOmission(this, expectedIntervalBetweenValueSamples);
    }


    /**
     * provide an overrideable point for initializing the state of TotalCount. Useful for
     * implementations that would represent totalCount as something other than a primitive value
     * (e.g. AtomicHistogram).
     */
    void initTotalCount() {
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
    public AbstractHistogram(final long lowestTrackableValue, final long highestTrackableValue, final int numberOfSignificantValueDigits) {
        // Verify argument validity
        if (lowestTrackableValue < 1) {
            throw new IllegalArgumentException("lowestTrackableValue must be >= 1");
        }
        if (highestTrackableValue < 2 * lowestTrackableValue) {
            throw new IllegalArgumentException("highestTrackableValue must be >= 2 * lowestTrackableValue");
        }
        if ((numberOfSignificantValueDigits < 0) || (numberOfSignificantValueDigits > 5)) {
            throw new IllegalArgumentException("numberOfSignificantValueDigits must be between 0 and 6");
        }
        identityCount = constructionIdentityCount.getAndIncrement();
        initTotalCount();
        init(lowestTrackableValue, highestTrackableValue, numberOfSignificantValueDigits, 0);
    }

    private void init(final long lowestTrackableValue, final long highestTrackableValue, final int numberOfSignificantValueDigits, long totalCount) {
        this.highestTrackableValue = highestTrackableValue;
        this.numberOfSignificantValueDigits = numberOfSignificantValueDigits;
        this.lowestTrackableValue = lowestTrackableValue;

        final long largestValueWithSingleUnitResolution = 2 * (long) Math.pow(10, numberOfSignificantValueDigits);

        unitMagnitude = (int) Math.floor(Math.log(lowestTrackableValue)/Math.log(2));

        // We need to maintain power-of-two subBucketCount (for clean direct indexing) that is large enough to
        // provide unit resolution to at least largestValueWithSingleUnitResolution. So figure out
        // largestValueWithSingleUnitResolution's nearest power-of-two (rounded up), and use that:
        int subBucketCountMagnitude = (int) Math.ceil(Math.log(largestValueWithSingleUnitResolution)/Math.log(2));
        subBucketHalfCountMagnitude = ((subBucketCountMagnitude > 1) ? subBucketCountMagnitude : 1) - 1;
        subBucketCount = (int) Math.pow(2, (subBucketHalfCountMagnitude + 1));
        subBucketHalfCount = subBucketCount / 2;
        subBucketMask = (subBucketCount - 1) << unitMagnitude;


        // determine exponent range needed to support the trackable value with no overflow:
        long trackableValue = (subBucketCount - 1) << unitMagnitude;
        int bucketsNeeded = 1;
        while (trackableValue < highestTrackableValue) {
            trackableValue <<= 1;
            bucketsNeeded++;
        }
        this.bucketCount = bucketsNeeded;

        countsArrayLength = (bucketCount + 1) * (subBucketCount / 2);

        setTotalCount(totalCount);

        histogramData = new HistogramData(this);
    }


    /**
     * get the configured lowestTrackableValue
     * @return lowestTrackableValue
     */
    public long getLowestTrackableValue() {
        return lowestTrackableValue;
    }

    /**
     * get the configured highestTrackableValue
     * @return highestTrackableValue
     */
    public long getHighestTrackableValue() {
        return highestTrackableValue;
    }

    /**
     * get the configured numberOfSignificantValueDigits
     * @return numberOfSignificantValueDigits
     */
    public int getNumberOfSignificantValueDigits() {
        return numberOfSignificantValueDigits;
    }


    private int countsArrayIndex(final int bucketIndex, final int subBucketIndex) {
        assert(subBucketIndex < subBucketCount);
        assert(bucketIndex == 0 || (subBucketIndex >= subBucketHalfCount));
        // Calculate the index for the first entry in the bucket:
        // (The following is the equivalent of ((bucketIndex + 1) * subBucketHalfCount) ):
        int bucketBaseIndex = (bucketIndex + 1) << subBucketHalfCountMagnitude;
        // Calculate the offset in the bucket:
        int offsetInBucket = subBucketIndex - subBucketHalfCount;
        // The following is the equivalent of ((subBucketIndex  - subBucketHalfCount) + bucketBaseIndex;
        return bucketBaseIndex + offsetInBucket;
    }

    long getCountAt(final int bucketIndex, final int subBucketIndex) {
        return getCountAtIndex(countsArrayIndex(bucketIndex, subBucketIndex));
    }

    private static void arrayAdd(final AbstractHistogram toHistogram, final AbstractHistogram fromHistogram) {
        if (fromHistogram.countsArrayLength != toHistogram.countsArrayLength) throw new IndexOutOfBoundsException();
        for (int i = 0; i < fromHistogram.countsArrayLength; i++)
            toHistogram.addToCountAtIndex(i, fromHistogram.getCountAtIndex(i));
    }

    int getBucketIndex(final long value) {
        int pow2ceiling = 64 - Long.numberOfLeadingZeros(value | subBucketMask); // smallest power of 2 containing value
        return  pow2ceiling - unitMagnitude - (subBucketHalfCountMagnitude + 1);
    }

    int getSubBucketIndex(long value, int bucketIndex) {
        return  (int)(value >> (bucketIndex + unitMagnitude));
    }

    private void recordCountAtValue(final long count, final long value) throws ArrayIndexOutOfBoundsException {
        // Dissect the value into bucket and sub-bucket parts, and derive index into counts array:
        int bucketIndex = getBucketIndex(value);
        int subBucketIndex = getSubBucketIndex(value, bucketIndex);
        int countsIndex = countsArrayIndex(bucketIndex, subBucketIndex);
        addToCountAtIndex(countsIndex, count);
        addToTotalCount(count);
    }

    private void recordSingleValue(final long value) throws ArrayIndexOutOfBoundsException {
        // Dissect the value into bucket and sub-bucket parts, and derive index into counts array:
        int bucketIndex = getBucketIndex(value);
        int subBucketIndex = getSubBucketIndex(value, bucketIndex);
        int countsIndex = countsArrayIndex(bucketIndex, subBucketIndex);
        incrementCountAtIndex(countsIndex);
        incrementTotalCount();
    }


    private void recordValueWithCountAndExpectedInterval(final long value, final long count,
                                                         final long expectedIntervalBetweenValueSamples) throws ArrayIndexOutOfBoundsException {
        recordCountAtValue(count, value);
        if (expectedIntervalBetweenValueSamples <=0)
            return;
        for (long missingValue = value - expectedIntervalBetweenValueSamples;
             missingValue >= expectedIntervalBetweenValueSamples;
             missingValue -= expectedIntervalBetweenValueSamples) {
            recordCountAtValue(count, missingValue);
        }
    }

    /**
     * Record a value in the histogram.
     * <p>
     * To compensate for the loss of sampled values when a recorded value is larger than the expected
     * interval between value samples, Histogram will auto-generate an additional series of decreasingly-smaller
     * (down to the expectedIntervalBetweenValueSamples) value records.
     * <p>
     * Note: This is a at-recording correction method, as opposed to the post-recording correction method provided
     * by {@link #copyCorrectedForCoordinatedOmission(long) getHistogramCorrectedForCoordinatedOmission}.
     * The two methods are mutually exclusive, and only one of the two should be be used on a given data set to correct
     * for the same coordinated omission issue.
     * <p>
     * See notes in the description of the Histogram calls for an illustration of why this corrective behavior is
     * important.
     *
     * @param value The value to record
     * @param expectedIntervalBetweenValueSamples If expectedIntervalBetweenValueSamples is larger than 0, add
     *                                           auto-generated value records as appropriate if value is larger
     *                                           than expectedIntervalBetweenValueSamples
     * @throws ArrayIndexOutOfBoundsException
     */
    public void recordValueWithExpectedInterval(final long value, final long expectedIntervalBetweenValueSamples) throws ArrayIndexOutOfBoundsException {
        recordValueWithCountAndExpectedInterval(value, 1, expectedIntervalBetweenValueSamples);
    }

    /**
     * @deprecated
     *
     * Record a value in the histogram. This deprecated method has identical behavior to
     * <b><code>recordValueWithExpectedInterval()</code></b>. It was renamed to avoid ambiguity.
     *
     * @param value The value to record
     * @param expectedIntervalBetweenValueSamples If expectedIntervalBetweenValueSamples is larger than 0, add
     *                                           auto-generated value records as appropriate if value is larger
     *                                           than expectedIntervalBetweenValueSamples
     * @throws ArrayIndexOutOfBoundsException
     */
    public void recordValue(final long value, final long expectedIntervalBetweenValueSamples) throws ArrayIndexOutOfBoundsException {
        recordValueWithExpectedInterval(value, expectedIntervalBetweenValueSamples);
    }


    /**
     * Record a value in the histogram (adding to the value's current count)
     *
     * @param value The value to be recorded
     * @param count The number of occurrences of this value to record
     * @throws ArrayIndexOutOfBoundsException
     */
    public void recordValueWithCount(final long value, final long count) throws ArrayIndexOutOfBoundsException {
        recordCountAtValue(count, value);
    }

    /**
     * Record a value in the histogram
     *
     * @param value The value to be recorded
     * @throws ArrayIndexOutOfBoundsException
     */
    public void recordValue(final long value) throws ArrayIndexOutOfBoundsException {
        recordSingleValue(value);
    }

    /**
     * Reset the contents and stats of this histogram
     */
    public void reset() {
        clearCounts();
    }

    /**
     * Add the contents of another histogram to this one
     *
     * @param fromHistogram The other histogram. highestTrackableValue and largestValueWithSingleUnitResolution must match.
     */
    public void add(final AbstractHistogram fromHistogram) {
        if ((highestTrackableValue != fromHistogram.highestTrackableValue) ||
                (numberOfSignificantValueDigits != fromHistogram.numberOfSignificantValueDigits) ||
                (bucketCount != fromHistogram.bucketCount) ||
                (subBucketCount != fromHistogram.subBucketCount))
            throw new IllegalArgumentException("Cannot add histograms with incompatible ranges");
        arrayAdd(this, fromHistogram);
        setTotalCount(getTotalCount() + fromHistogram.getTotalCount());
    }

    /**
     * Add the contents of another histogram to this one, while correcting the incoming data for coordinated omission.
     * <p>
     * To compensate for the loss of sampled values when a recorded value is larger than the expected
     * interval between value samples, the values added will include an auto-generated additional series of
     * decreasingly-smaller (down to the expectedIntervalBetweenValueSamples) value records for each count found
     * in the current histogram that is larger than the expectedIntervalBetweenValueSamples.
     *
     * Note: This is a post-recording correction method, as opposed to the at-recording correction method provided
     * by {@link #recordValueWithExpectedInterval(long, long) recordValueWithExpectedInterval}. The two
     * methods are mutually exclusive, and only one of the two should be be used on a given data set to correct
     * for the same coordinated omission issue.
     * by
     * <p>
     * See notes in the description of the Histogram calls for an illustration of why this corrective behavior is
     * important.
     *
     * @param fromHistogram The other histogram. highestTrackableValue and largestValueWithSingleUnitResolution must match.
     * @param expectedIntervalBetweenValueSamples If expectedIntervalBetweenValueSamples is larger than 0, add
     *                                           auto-generated value records as appropriate if value is larger
     *                                           than expectedIntervalBetweenValueSamples
     * @throws ArrayIndexOutOfBoundsException
     */
    public void addWhileCorrectingForCoordinatedOmission(final AbstractHistogram fromHistogram, final long expectedIntervalBetweenValueSamples) {
        final AbstractHistogram toHistogram = this;

        for (HistogramIterationValue v : fromHistogram.getHistogramData().recordedValues()) {
            toHistogram.recordValueWithCountAndExpectedInterval(v.getValueIteratedTo(),
                    v.getCountAtValueIteratedTo(), expectedIntervalBetweenValueSamples);
        }
    }

    /**
     * Determine if this histogram had any of it's value counts overflow.
     * Since counts are kept in fixed integer form with potentially limited range (e.g. int and short), a
     * specific value range count could potentially overflow, leading to an inaccurate and misleading histogram
     * representation. This method accurately determines whether or not an overflow condition has happened in an
     * IntHistogram or ShortHistogram.
     *
     * @return True if this histogram has had a count value overflow.
     */
    public boolean hasOverflowed() {
        // On overflow, the totalCount accumulated counter will (always) not match the total of counts
        long totalCounted = 0;
        for (int i = 0; i < countsArrayLength; i++) {
            totalCounted += getCountAtIndex(i);
        }
        return (totalCounted != getTotalCount());
    }

    /**
     * Reestablish the internal notion of totalCount by recalculating it from recorded values.
     *
     * Implementations of AbstractHistogram may maintain a separately tracked notion of totalCount,
     * which is useful for concurrent modification tracking, overflow detection, and speed of execution
     * in iteration. This separately tracked totalCount can get into a state that is inconsistent with
     * the currently recorded value counts under various concurrent modification and overflow conditions.
     *
     * Applying this method will override internal indications of potential overflows and concurrent
     * modification, and will reestablish a self-consistent representation of the histogram data
     * based purely on the current internal representation of recorded counts.
     * <p>
     * In cases of concurrent modifications such as during copying, or due to racy multi-threaded
     * updates on non-atomic or non-synchronized variants, which can result in potential loss
     * of counts and an inconsistent (indicating potential overflow) internal state, calling this
     * method on a histogram will reestablish a consistent internal state based on the potentially
     * lossy counts representations.
     * <p>
     * Note that this method is not synchronized against concurrent modification in any way,
     * and will only reliably reestablish consistent internal state when no concurrent modification
     * of the histogram is performed while it executes.
     * <p>
     * Note that in the cases of actual overflow conditions (which can result in negative counts)
     * this self consistent view may be very wrong, and not just slightly lossy.
     *
     */
    public void reestablishTotalCount() {
        // On overflow, the totalCount accumulated counter will (always) not match the total of counts
        long totalCounted = 0;
        for (int i = 0; i < countsArrayLength; i++) {
            totalCounted += getCountAtIndex(i);
        }
        setTotalCount(totalCounted);
    }

    /**
     * Determine if this histogram is equivalent to another.
     *
     * @param other the other histogram to compare to
     * @return True if this histogram are equivalent with the other.
     */
    public boolean equals(Object other){
        if ( this == other ) return true;
        if ( !(other instanceof AbstractHistogram) ) return false;
        AbstractHistogram that = (AbstractHistogram)other;
        if ((highestTrackableValue != that.highestTrackableValue) ||
                (numberOfSignificantValueDigits != that.numberOfSignificantValueDigits))
            return false;
        if (countsArrayLength != that.countsArrayLength)
            return false;
        if (getTotalCount() != that.getTotalCount())
            return false;
        return true;
    }

    /**
     * Provide access to the histogram's data set.
     * @return a {@link HistogramData} that can be used to query stats and iterate through the default (corrected)
     * data set.
     */
    public HistogramData getHistogramData() {
        return histogramData;
    }

    /**
     * Get the size (in value units) of the range of values that are equivalent to the given value within the
     * histogram's resolution. Where "equivalent" means that value samples recorded for any two
     * equivalent values are counted in a common total count.
     *
     * @param value The given value
     * @return The lowest value that is equivalent to the given value within the histogram's resolution.
     */
    public long sizeOfEquivalentValueRange(final long value) {
        int bucketIndex = getBucketIndex(value);
        int subBucketIndex = getSubBucketIndex(value, bucketIndex);
        long distanceToNextValue =
                (1 << ( unitMagnitude + ((subBucketIndex >= subBucketCount) ? (bucketIndex + 1) : bucketIndex)));
        return distanceToNextValue;
    }

    /**
     * Get the lowest value that is equivalent to the given value within the histogram's resolution.
     * Where "equivalent" means that value samples recorded for any two
     * equivalent values are counted in a common total count.
     *
     * @param value The given value
     * @return The lowest value that is equivalent to the given value within the histogram's resolution.
     */
    public long lowestEquivalentValue(final long value) {
        int bucketIndex = getBucketIndex(value);
        int subBucketIndex = getSubBucketIndex(value, bucketIndex);
        long thisValueBaseLevel = valueFromIndex(bucketIndex, subBucketIndex, unitMagnitude);
        return thisValueBaseLevel;
    }

    /**
     * Get the highest value that is equivalent to the given value within the histogram's resolution.
     * Where "equivalent" means that value samples recorded for any two
     * equivalent values are counted in a common total count.
     *
     * @param value The given value
     * @return The highest value that is equivalent to the given value within the histogram's resolution.
     */
    public long highestEquivalentValue(final long value) {
        return nextNonEquivalentValue(value) - 1;
    }

    /**
     * Get a value that lies in the middle (rounded up) of the range of values equivalent the given value.
     * Where "equivalent" means that value samples recorded for any two
     * equivalent values are counted in a common total count.
     *
     * @param value The given value
     * @return The value lies in the middle (rounded up) of the range of values equivalent the given value.
     */
    public long medianEquivalentValue(final long value) {
        return (lowestEquivalentValue(value) + (sizeOfEquivalentValueRange(value) >> 1));
    }

    /**
     * Get the next value that is not equivalent to the given value within the histogram's resolution.
     * Where "equivalent" means that value samples recorded for any two
     * equivalent values are counted in a common total count.
     *
     * @param value The given value
     * @return The next value that is not equivalent to the given value within the histogram's resolution.
     */
    public long nextNonEquivalentValue(final long value) {
        return lowestEquivalentValue(value) + sizeOfEquivalentValueRange(value);
    }

    /**
     * Determine if two values are equivalent with the histogram's resolution.
     * Where "equivalent" means that value samples recorded for any two
     * equivalent values are counted in a common total count.
     *
     * @param value1 first value to compare
     * @param value2 second value to compare
     * @return True if values are equivalent with the histogram's resolution.
     */
    public boolean valuesAreEquivalent(final long value1, final long value2) {
        return (lowestEquivalentValue(value1) == lowestEquivalentValue(value2));
    }

    private static final long serialVersionUID = 42L;

    private void writeObject(final ObjectOutputStream o)
            throws IOException
    {
        o.writeLong(lowestTrackableValue);
        o.writeLong(highestTrackableValue);
        o.writeInt(numberOfSignificantValueDigits);
        o.writeLong(getTotalCount()); // Needed because overflow situations may lead this to differ from counts totals
    }

    private void readObject(final ObjectInputStream o)
            throws IOException, ClassNotFoundException {
        final long lowestTrackableValue = o.readLong();
        final long highestTrackableValue = o.readLong();
        final int numberOfSignificantValueDigits = o.readInt();
        final long totalCount = o.readLong();
        init(lowestTrackableValue, highestTrackableValue, numberOfSignificantValueDigits, totalCount);
        setTotalCount(totalCount);
    }


    static final long valueFromIndex(int bucketIndex, int subBucketIndex, int unitMagnitude)
    {
        return ((long) subBucketIndex) << ( bucketIndex + unitMagnitude);
    }

    public byte[] toCompressedBytes(CompressionStrategy strategy) {
        byte[] array = toUncompressedBytes();
        try {
            return strategy.compress(array);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public byte[] toUncompressedBytes() {
        ByteBuffer buf = ByteBuffer.allocate(8 * countsArrayLength + (3 * 8) + 4);
        buf.order(ByteOrder.LITTLE_ENDIAN);
        buf.putLong(lowestTrackableValue);
        buf.putLong(highestTrackableValue);
        buf.putInt(numberOfSignificantValueDigits);
        buf.putLong(getTotalCount());
        for (int ii = 0; ii < countsArrayLength; ii++) {
            buf.putLong(getCountAtIndex(ii));
        }
        return buf.array();
    }

    public static byte[] toCompressedBytes(byte bytes[], CompressionStrategy strategy) {
        try {
            return strategy.compress(bytes);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Histogram fromCompressedBytes(byte bytes[], CompressionStrategy strategy) {
        try {
            ByteBuffer buf = ByteBuffer.wrap(strategy.uncompress(bytes));
            buf.order(ByteOrder.LITTLE_ENDIAN);
            final long lTrackableValue = buf.getLong();
            final long hTrackableValue = buf.getLong();
            final int nSVD = buf.getInt();
            Histogram h = new Histogram(lTrackableValue, hTrackableValue, nSVD);
            h.addToTotalCount(buf.getLong());
            for (int ii = 0; ii < h.countsArrayLength; ii++) {
                h.addToCountAtIndex(ii, buf.getLong());
            }
            return h;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}