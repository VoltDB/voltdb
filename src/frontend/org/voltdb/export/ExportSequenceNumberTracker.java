/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

import java.util.Iterator;

import org.voltcore.utils.Pair;

import com.google_voltpatches.common.collect.BoundType;
import com.google_voltpatches.common.collect.DiscreteDomain;
import com.google_voltpatches.common.collect.Range;
import com.google_voltpatches.common.collect.RangeSet;
import com.google_voltpatches.common.collect.TreeRangeSet;

public class ExportSequenceNumberTracker {
    protected RangeSet<Long> m_map;

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
     */
    public void addRange(long start, long end) {
        m_map.add(range(start, end));
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
     * is before the first sequence number of the tracker, it's a no-op.
     * @param newTruncationPoint    New safe point
     */
    public void truncate(long newTruncationPoint) {
        if (size() == 0) return;
        if (newTruncationPoint < getFirstSeqNo()) return;
        final Iterator<Range<Long>> iter = m_map.asRanges().iterator();
        while (iter.hasNext()) {
            final Range<Long> next = iter.next();
            if (end(next) < newTruncationPoint) {
                iter.remove();
            } else if (next.contains(newTruncationPoint)) {
                iter.remove();
                m_map.add(range(newTruncationPoint, end(next)));
                return;
            } else {
                break;
            }
        }
        m_map.add(range(newTruncationPoint, newTruncationPoint));
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
     * Get end sequence number of the range that contains given sequence number
     * @param seq
     * @return end sequence number of the range if exists, otherwise return Long.MIN_VALUE
     */
    public long getRangeContaining(long seq) {
        assert (!m_map.isEmpty());
        Range<Long> range = m_map.rangeContaining(seq);
        if (range != null) {
            return end(range);
        }
        return Long.MIN_VALUE;
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
        assert (!m_map.isEmpty());
        if (size() < 2) return null;
        Iterator<Range<Long>> iter = m_map.asRanges().iterator();
        long start = end(iter.next()) + 1;
        long end = start(iter.next()) - 1;
        return new Pair<Long, Long>(start, end);
    }

    RangeSet<Long> getRanges() {
        return m_map;
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
}
