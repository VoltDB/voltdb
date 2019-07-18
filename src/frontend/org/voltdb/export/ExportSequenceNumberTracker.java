/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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

package org.voltdb.export;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Set;

import org.voltcore.utils.DeferredSerialization;
import org.voltcore.utils.Pair;

import com.google_voltpatches.common.collect.BoundType;
import com.google_voltpatches.common.collect.DiscreteDomain;
import com.google_voltpatches.common.collect.Range;
import com.google_voltpatches.common.collect.RangeSet;
import com.google_voltpatches.common.collect.TreeRangeSet;

public class ExportSequenceNumberTracker implements DeferredSerialization {
    protected RangeSet<Long> m_map;
    // Is the first sequence a sentinel? Sentinel doesn't count into the total sequence size of tracker.
    private boolean m_hasSentinel = false;

    /**
     * Returns a canonical range that can be added to the internal range
     * set. Only ranges returned by this method can be added to the range set,
     * otherwise range operations like contains may yield unexpected
     * results. Consult the Guava doc on Range for details.
     *
     * @param start    Start of the range
     * @param end      End of the range
     * @return Canonical range
     */
    static Range<Long> range(long start, long end) {
        return Range.closed(start, end).canonical(DiscreteDomain.longs());
    }

    /**
     * Get the start of the range. Always use this method to get the start of a
     * range because it respects the bound type.
     * @param range
     * @return Start of the range
     */
    private static long start(Range<Long> range) {
        if (range.lowerBoundType() == BoundType.OPEN) {
            return DiscreteDomain.longs().next(range.lowerEndpoint());
        } else {
            return range.lowerEndpoint();
        }
    }

    /**
     * Get the end of the range. Always use this method to get the end of a
     * range because it respects the bound type.
     * @param range
     * @return End of the range
     */
    private static long end(Range<Long> range) {
        if (range.upperBoundType() == BoundType.OPEN) {
            return DiscreteDomain.longs().previous(range.upperEndpoint());
        } else {
            return range.upperEndpoint();
        }
    }

    public ExportSequenceNumberTracker() {
        m_map = TreeRangeSet.create();
    }

    public ExportSequenceNumberTracker(ExportSequenceNumberTracker other) {
        m_map = TreeRangeSet.create(other.m_map);
    }

    public int size() {
        return m_map.asRanges().size();
    }

    /**
     * Add a range to the tracker.
     * @param start
     * @param end
     * @return number of non-overlapped sequence being added
     */
    public long addRange(long start, long end) {
        Range<Long> newRange = range(start, end);
        long nonOverlapSize = end - start + 1;
        if (m_map.intersects(newRange)) {
             for (Range<Long> next : m_map.asRanges()) {
                 if (next.isConnected(newRange)) {
                     Range<Long> intersection = next.intersection(newRange);
                     nonOverlapSize -= end(intersection) - start(intersection) + 1;
                 }
             }
        }
        m_map.add(newRange);
        return nonOverlapSize;
    }

    /**
     * Appends a range to the tracker. The range has to be after the last sequence number
     * of the tracker.
     * @param start
     * @param end
     */
    public void append(long start, long end) {
        assert(start <= end && (m_map.isEmpty() || start > end(m_map.span())));
        addRange(start, end);
    }

    /**
     * Truncate the tracker to the given safe point. After truncation, the new
     * safe point will be the first sequence number of the tracker. If the new safe point
     * is before the first sequence number of the tracker, it's a no-op. If the
     * map is empty, truncation point will be the new safe point of tracker.
     * @param newTruncationPoint    New safe point
     * @return number of sequence be truncated
     */
    public int truncate(long newTruncationPoint) {
        int truncated = 0;
        if (m_map.isEmpty()) {
            m_map.add(range(newTruncationPoint, newTruncationPoint));
            m_hasSentinel = true;
            return truncated;
        }
        if (newTruncationPoint < getFirstSeqNo()) {
            return truncated;
        }
        // Sentinel doesn't count as valid sequence
        if (m_hasSentinel) {
            truncated -= 1;
        }
        final Iterator<Range<Long>> iter = m_map.asRanges().iterator();
        while (iter.hasNext()) {
            final Range<Long> next = iter.next();
            if (end(next) < newTruncationPoint) {
                truncated += end(next) - start(next) + 1;
                iter.remove();
            } else if (next.contains(newTruncationPoint)) {
                truncated += newTruncationPoint - start(next) + 1;
                iter.remove();
                m_map.add(range(newTruncationPoint, end(next)));
                m_hasSentinel = true;
                return truncated;
            } else {
                break;
            }
        }
        if (!m_map.contains(newTruncationPoint)) {
            m_map.add(range(newTruncationPoint, newTruncationPoint));
            m_hasSentinel = true;
        }
        return truncated;
    }

    public Set<Range<Long>> getRanges() {
        return m_map.asRanges();
    }

    /**
     * Truncate the tracker to the given truncation point. After truncation,
     * any ranges after the new truncation point will be removed.
     * If the new safe point is after the last sequence number of the tracker,
     * it's a no-op. If the map is empty, truncation point will be the new safe point of tracker.
     * @param newTruncationPoint    New safe point
     */
    public void truncateAfter(long newTruncationPoint) {
        if (size() == 0) {
            m_map.add(range(newTruncationPoint, newTruncationPoint));
            m_hasSentinel = true;
            return;
        }
        if (newTruncationPoint > getLastSeqNo()) {
            return;
        }
        final Iterator<Range<Long>> iter = m_map.asDescendingSetOfRanges().iterator();
        while (iter.hasNext()) {
            final Range<Long> next = iter.next();
            if (start(next) > newTruncationPoint) {
                iter.remove();
            } else if (next.contains(newTruncationPoint)) {
                iter.remove();
                m_map.add(range(start(next), newTruncationPoint));
                return;
            } else {
                break;
            }
        }
        if (m_map.isEmpty()) {
            m_map.add(range(newTruncationPoint, newTruncationPoint));
            m_hasSentinel = true;
        }
    }

    /**
     * Merge the given tracker with the current tracker. Ranges can
     * overlap. After the merge, the current tracker will be truncated to the
     * larger safe point.
     * @param tracker
     */
    public void mergeTracker(ExportSequenceNumberTracker tracker) {
        m_map.addAll(tracker.m_map);
    }

    /**
     * Check if this tracker contains the given range. If the given range is
     * before the safe point, it always returns true.
     * @return true if the given range can be covered by the tracker.
     */
    public boolean contains(long start, long end) {
        assert start <= end;
        return end <= getSafePoint() || m_map.encloses(range(start, end));
    }

    /**
     * Get range that contains given sequence number
     * @param seq
     * @return a pair of start and end sequence number of the range if it exists,
     *         otherwise return null
     */
    public Pair<Long, Long> getRangeContaining(long seq) {
        Range<Long> range = m_map.rangeContaining(seq);
        if (range != null) {
            return new Pair<Long, Long>(start(range), end(range));
        }
        return null;
    }

    /**
     * Get the current safe point for acking. There is no gap before this point
     * in this tracker.
     * @return The current safe-to-ack DrId
     */
    public long getSafePoint() {
        assert (!m_map.isEmpty());
        return end(m_map.asRanges().iterator().next());
    }

    /**
     * Find range of the first gap if it exists.
     * If there is only one entry, range after the first entry is NOT a gap.
     * @return start and end sequence number of the first gap, if gap doesn't
     *         exist return null
     */
    public Pair<Long, Long> getFirstGap() {
        return (getFirstGap(0L));
    }

    /**
     * Find first gap after or including a sequence number if it exists
     *
     * @param afterSeqNo find first gap after (or including) this seqNo
     * @return
     */
    public Pair<Long, Long> getFirstGap(long afterSeqNo) {
        if (m_map.isEmpty() || size() < 2) {
            return null;
        }
        Iterator<Range<Long>> iter = m_map.asRanges().iterator();
        Range<Long> current = iter.next();
        assert current != null;

        while (iter.hasNext()) {
            Range<Long> next = iter.next();
            long start = end(current) + 1;
            long end = start(next) - 1;
            if (end < afterSeqNo) {
                current = next;
                continue;
            }
            return new Pair<Long, Long>(start, end);
        }
        return null;
    }

    /**
     * Get the first sequence number of the tracker. It will always be smaller than or
     * equal to the current safe point.
     */
    public long getFirstSeqNo() {
        return start(m_map.span());
    }

    /**
     * Get the last sequence number of the tracker. It will always be greater than or equal
     * to the current safe point.
     */
    public long getLastSeqNo() {
        return end(m_map.span());
    }

    /**
     * Get total number of sequence from the tracker.
     * @return
     */
    public int sizeInSequence() {
        int sequence = 0;
        if (m_map.isEmpty()) {
            return sequence;
        }
        final Iterator<Range<Long>> iter = m_map.asRanges().iterator();
        while (iter.hasNext()) {
            Range<Long> range = iter.next();
            sequence += end(range) - start(range) + 1;
        }
        if (m_hasSentinel) {
            sequence -= 1;
        }
        return sequence;
    }

    public String toShortString() {
        if (m_map.isEmpty()) {
            return "Empty Map";
        }
        StringBuilder sb = new StringBuilder();
        toShortString(sb);
        return sb.toString();
    }

    @Override
    public String toString() {
        if (m_map.isEmpty()) {
            return "Empty Map";
        }
        StringBuilder sb = new StringBuilder();
        toString(sb);
        return sb.toString();
    }

    protected void toShortString(StringBuilder sb) {
        sb.append("span [").append(getFirstSeqNo()).append("-");
        sb.append(getLastSeqNo());
        sb.append(", size=").append(size()).append("]");
    }

    protected void toString(StringBuilder sb) {
        for (Range<Long> entry : m_map.asRanges()) {
            sb.append("[").append(start(entry)).append(", ")
              .append(end(entry)).append("] ");
        }
    }

    public boolean isEmpty() {
        return m_map.isEmpty();
    }

    @Override
    public void serialize(ByteBuffer buf) throws IOException {

        // Entry count (int) + 2 Long per entry
        if (isEmpty()) {
            buf.putInt(0);
        } else {
            int trackSize = size();
            buf.putInt(trackSize);

            for (Range<Long> entry : m_map.asRanges()) {
                buf.putLong(start(entry));
                buf.putLong(end(entry));
            }
        }

        // Sentinel (byte)
        buf.put(m_hasSentinel ? (byte) 1 : (byte) 0);
    }

    @Override
    public void cancel() {
        // *void*
    }

    @Override
    public int getSerializedSize() throws IOException {
        int count = 0;

        // Entry count (int) + 2 Long per entry
        if (isEmpty()) {
            count += 4;
        } else {
            count += 4;
            count += 2 * 8 * size();
        }

        // Sentinel (byte)
        return count + 1;
    }

    public ExportSequenceNumberTracker duplicate() {

        ExportSequenceNumberTracker tracker = new ExportSequenceNumberTracker();

        for (Range<Long> entry : m_map.asRanges()) {
            tracker.append(start(entry), end(entry));
        }
        tracker.m_hasSentinel = m_hasSentinel;
        return tracker;
    }

    public static ExportSequenceNumberTracker deserialize(ByteBuffer buf) throws IOException {

        ExportSequenceNumberTracker tracker = new ExportSequenceNumberTracker();

        int count = buf.getInt();
        for (int i = 0; i < count; i++) {
            long start = buf.getLong();
            long end = buf.getLong();
            tracker.append(start, end);
        }
        tracker.m_hasSentinel = buf.get() == 1;
        return tracker;
    }
}
