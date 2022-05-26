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

package org.voltdb;

import static org.voltdb.DRLogSegmentId.isEmptyDRId;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltcore.logging.VoltLogger;
import org.voltdb.iv2.UniqueIdGenerator;

import com.google_voltpatches.common.collect.BoundType;
import com.google_voltpatches.common.collect.DiscreteDomain;
import com.google_voltpatches.common.collect.Range;
import com.google_voltpatches.common.collect.RangeSet;
import com.google_voltpatches.common.collect.TreeRangeSet;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/*
 * WARNING:
 * The implementation assumes that the range set is never completely empty in methods like
 * getSafePointDrId, getFirstDrId, getLastDrId. However, the assumption is based on a single thread
 * (DR partition buffer receiver thread) modifying and accessing this. When accessing this from other threads,
 * we will need to be careful to make sure to add proper checks to ensure safe access.
 */
public class DRConsumerDrIdTracker implements Serializable {
    private static final long serialVersionUID = -4057397384030151271L;

    /** The offset at which {@link #m_lastSpUniqueId} can be found in the result of {@link #serialize(ByteBuffer)} */
    public static final int LAST_SP_UNIQUE_ID_OFFSET = 0;
    /** The offset at which {@link #m_lastMpUniqueId} can be found in the result of {@link #serialize(ByteBuffer)} */
    public static final int LAST_MP_UNIQUE_ID_OFFSET = LAST_SP_UNIQUE_ID_OFFSET + Long.BYTES;

    protected RangeSet<Long> m_map;
    private long m_lastSpUniqueId;
    private long m_lastMpUniqueId;
    private int m_producerPartitionId;


    public static class DRSiteDrIdTracker extends DRConsumerDrIdTracker {
        private long m_lastReceivedLogId;

        private DRSiteDrIdTracker(long lastReceived, long spUniqueId, long mpUniqueId, int producerPartitionId) {
            super(spUniqueId, mpUniqueId, producerPartitionId);
            m_lastReceivedLogId = lastReceived;
        }

        public DRSiteDrIdTracker(DRSiteDrIdTracker other) {
            super(other);
            m_lastReceivedLogId = other.m_lastReceivedLogId;
        }

        public DRSiteDrIdTracker(JSONObject jsObj, boolean resetLastReceived) throws JSONException {
            super(jsObj);
            m_lastReceivedLogId = resetLastReceived ? 0L : jsObj.getLong("lastReceivedLogId");
        }

        public DRSiteDrIdTracker(ByteBuf buf, boolean resetLastReceived) {
            super(buf);
            m_lastReceivedLogId = buf.readLong();
            if (resetLastReceived) {
                m_lastReceivedLogId = 0;
            }
        }

        public long getLastReceivedLogId() {
            return m_lastReceivedLogId;
        }

        public void incLastReceivedLogId() {
            m_lastReceivedLogId++;
        }

        public JSONObject toJSON() throws JSONException {
            JSONObject obj = new JSONObject();
            super.toJSON(obj);
            obj.put("lastReceivedLogId", m_lastReceivedLogId);
            return obj;
        }

        @Override
        public String toShortString() {
            if (m_map.isEmpty()) {
                return "Empty Map";
            }
            StringBuilder sb = new StringBuilder();
            sb.append("lastReceivedLogId ").append(m_lastReceivedLogId).append(" ");
            super.toShortString(sb);
            return sb.toString();
        }

        @Override
        public String toString() {
            if (m_map.isEmpty()) {
                return "Empty Map";
            }
            StringBuilder sb = new StringBuilder();
            sb.append("lastReceivedLogId ").append(m_lastReceivedLogId).append(" ");
            super.toString(sb);
            return sb.toString();
        }

        @Override
        public int getSerializedSize() {
            return super.getSerializedSize() + Long.BYTES;
        }

        @Override
        public void serialize(ByteBuf buff) {
            super.serialize(buff);
            buff.writeLong(m_lastReceivedLogId);
        }
    }


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


    protected DRConsumerDrIdTracker(long spUniqueId, long mpUniqueId, int producerPartitionId) {
        m_map = TreeRangeSet.create();
        m_lastSpUniqueId = spUniqueId;
        m_lastMpUniqueId = mpUniqueId;
        m_producerPartitionId = producerPartitionId;
    }

    public static DRConsumerDrIdTracker createBufferTracker(long spUniqueId, long mpUniqueId, int producerPartitionId) {
        DRConsumerDrIdTracker newTracker = new DRConsumerDrIdTracker(spUniqueId, mpUniqueId, producerPartitionId);
        return newTracker;
    }

    public static DRSiteDrIdTracker createSiteTracker(long lastReceived, long initialAckPoint, long spUniqueId, long mpUniqueId, int producerPartitionId) {
        DRSiteDrIdTracker newTracker = new DRSiteDrIdTracker(lastReceived, spUniqueId, mpUniqueId, producerPartitionId);
        newTracker.addRange(initialAckPoint, initialAckPoint);
        return newTracker;
    }

    public static DRConsumerDrIdTracker createBufferReceiverTracker(long initialAckPoint, long spUniqueId, long mpUniqueId, int producerPartitionId) {
        DRConsumerDrIdTracker newTracker = new DRConsumerDrIdTracker(spUniqueId, mpUniqueId, producerPartitionId);
        newTracker.addRange(initialAckPoint, initialAckPoint);
        return newTracker;
    }

    public boolean isRealTracker() {
        return (m_lastSpUniqueId > 0) || (m_lastMpUniqueId > 0);
    }

    public boolean isDummyTracker() {
        return (m_lastSpUniqueId == Long.MIN_VALUE) && (m_lastMpUniqueId == Long.MIN_VALUE) && isEmptyDRId(getLastDrId());
    }

    public DRConsumerDrIdTracker(DRConsumerDrIdTracker other) {
        m_map = TreeRangeSet.create(other.m_map);
        m_lastSpUniqueId = other.m_lastSpUniqueId;
        m_lastMpUniqueId = other.m_lastMpUniqueId;
        m_producerPartitionId = other.m_producerPartitionId;
    }

    public DRConsumerDrIdTracker(JSONObject jsObj) throws JSONException {
        m_map = TreeRangeSet.create();
        m_lastSpUniqueId = jsObj.getLong("spUniqueId");
        m_lastMpUniqueId = jsObj.getLong("mpUniqueId");
        m_producerPartitionId = jsObj.getInt("producerPartitionId");
        final JSONArray drIdRanges = jsObj.getJSONArray("drIdRanges");
        for (int ii = 0; ii < drIdRanges.length(); ii++) {
            JSONObject obj = drIdRanges.getJSONObject(ii);
            String startDrIdStr = obj.keys().next();
            m_map.add(range(Long.valueOf(startDrIdStr), obj.getLong(startDrIdStr)));
        }
    }

    public DRConsumerDrIdTracker(ByteBuf buff) {
        m_map = TreeRangeSet.create();
        m_lastSpUniqueId = buff.readLong();
        m_lastMpUniqueId = buff.readLong();
        m_producerPartitionId = buff.readInt();
        int mapSize = buff.readInt();
        for (int ii=0; ii<mapSize; ii++) {
            m_map.add(range(buff.readLong(), buff.readLong()));
        }
    }

    public DRConsumerDrIdTracker(byte[] flattened) {
        this(Unpooled.wrappedBuffer(flattened));
    }

    public int getProducerPartitionId() {
        return m_producerPartitionId;
    }

    public int getSerializedSize() {
        return 8        // m_lastSpUniqueId
             + 8        // m_lastMpUniqueId
             + 4        // m_producerPartitionId
             + 4        // map size
             + (m_map.asRanges().size() * 16);
    }

    public void serialize(ByteBuf buff) {
        if (buff.maxWritableBytes() < getSerializedSize()) {
            System.err.println(buff + " " + buff.maxWritableBytes());
        }
        assert (buff.maxWritableBytes() >= getSerializedSize()) : buff;
        buff.writeLong(m_lastSpUniqueId);
        buff.writeLong(m_lastMpUniqueId);
        buff.writeInt(m_producerPartitionId);
        buff.writeInt(m_map.asRanges().size());
        for(Range<Long> entry : m_map.asRanges()) {
            buff.writeLong(start(entry));
            buff.writeLong(end(entry));
        }
    }

    public void serialize(byte[] flattened) {
        serialize(Unpooled.wrappedBuffer(flattened).writerIndex(0));
    }

    /**
    * Serialize this {@code DRConsumerDrIdTracker} instance.
    *
    * @serialData The last spUnique id and mpUniqueId is emitted ({@code long}), followed by size of
    * the range set ({@code int}) (number of ranges) , followed by each individual range that includes
    * start ({@code long}) and end ({@code long})
    */
    private void writeObject(ObjectOutputStream out) throws IOException {
        out.writeLong(m_lastSpUniqueId);
        out.writeLong(m_lastMpUniqueId);
        out.writeShort(m_producerPartitionId);
        out.writeInt(m_map.asRanges().size());
        for(Range<Long> entry : m_map.asRanges()) {
            out.writeLong(start(entry));
            out.writeLong(end(entry));
        }
    }

    private void readObject(ObjectInputStream in) throws IOException {
        m_map = TreeRangeSet.create();
        m_lastSpUniqueId = in.readLong();
        m_lastMpUniqueId = in.readLong();
        m_producerPartitionId = in.readShort();
        int mapSize = in.readInt();
        for (int ii = 0; ii < mapSize; ii++) {
            m_map.add(range(in.readLong(), in.readLong()));
        }
    }

    public int size() {
        return m_map.asRanges().size();
    }

    /**
     * Add a range to the tracker.
     * @param startDrId
     * @param endDrId
     */
    public void addRange(long startDrId, long endDrId) {
        m_map.add(range(startDrId, endDrId));
    }

    /**
     * Add a range to the tracker.
     * @param startDrId
     * @param endDrId
     * @param spUniqueId
     * @param mpUniqueId
     */
    public void addRange(long startDrId, long endDrId, long spUniqueId, long mpUniqueId) {
        // Note that any given range or tracker could have either sp or mp sentinel values
        m_lastSpUniqueId = Math.max(m_lastSpUniqueId, spUniqueId);
        m_lastMpUniqueId = Math.max(m_lastMpUniqueId, mpUniqueId);
        addRange(startDrId, endDrId);
    }

    /**
     * Appends a range to the tracker. The range has to be after the last DrId
     * of the tracker.
     * @param startDrId
     * @param endDrId
     * @param spUniqueId
     * @param mpUniqueId
     */
    public void append(long startDrId, long endDrId, long spUniqueId, long mpUniqueId) {
        assert(startDrId <= endDrId && (m_map.isEmpty() || startDrId > end(m_map.span())));

        addRange(startDrId, endDrId, spUniqueId, mpUniqueId);
    }

    /**
     * Truncate the tracker to the given safe point. After truncation, the new
     * safe point will be the first DrId of the tracker. If the new safe point
     * is before the first DrId of the tracker, it's a no-op.
     * @param newTruncationPoint    New safe point
     */
    public void truncate(long newTruncationPoint) {
        if (newTruncationPoint < getFirstDrId()) {
            return;
        }
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
    public void mergeTracker(DRConsumerDrIdTracker tracker) {
        final long newSafePoint = Math.max(tracker.getSafePointDrId(), getSafePointDrId());
        m_map.addAll(tracker.m_map);
        truncate(newSafePoint);
        m_lastSpUniqueId = Math.max(m_lastSpUniqueId, tracker.m_lastSpUniqueId);
        m_lastMpUniqueId = Math.max(m_lastMpUniqueId, tracker.m_lastMpUniqueId);
    }

    /**
     * Check if this tracker contains the given range. If the given range is
     * before the safe point, it always returns true.
     * @return true if the given range can be covered by the tracker.
     */
    public boolean contains(long startDrId, long endDrId) {
        assert startDrId <= endDrId;
        return endDrId <= getSafePointDrId() || m_map.encloses(range(startDrId, endDrId));
    }

    /**
     * Get the current safe point for acking. There is no gap before this point
     * in this tracker.
     * @return The current safe-to-ack DrId
     */
    public long getSafePointDrId() {
        assert (!m_map.isEmpty());
        return end(m_map.asRanges().iterator().next());
    }

    public long getLastSpUniqueId() {
        return m_lastSpUniqueId;
    }

    public long getLastMpUniqueId() {
        return m_lastMpUniqueId;
    }

    RangeSet<Long> getDrIdRanges() {
        return m_map;
    }

    /**
     * Get the first DrId of the tracker. It will always be smaller than or
     * equal to the current safe point.
     */
    public long getFirstDrId() {
        return start(m_map.span());
    }

    /**
     * Get the last DrId of the tracker. It will always be greater than or equal
     * to the current safe point.
     */
    public long getLastDrId() {
        return end(m_map.span());
    }

    protected void toJSON(JSONObject obj) throws JSONException
    {
        obj.put("spUniqueId", m_lastSpUniqueId);
        obj.put("mpUniqueId", m_lastMpUniqueId);
        obj.put("producerPartitionId", m_producerPartitionId);
        JSONArray drIdRanges = new JSONArray();
        for (Range<Long> sequenceRange : m_map.asRanges()) {
            JSONObject range = new JSONObject();
            range.put(Long.toString(start(sequenceRange)), end(sequenceRange));
            drIdRanges.put(range);
        }
        obj.put("drIdRanges", drIdRanges);
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
        sb.append("lastSpUniqueId ").append(UniqueIdGenerator.toShortString(m_lastSpUniqueId)).append(" ");
        sb.append("lastMpUniqueId ").append(UniqueIdGenerator.toShortString(m_lastMpUniqueId)).append(" ");
        sb.append("producerPartitionId ").append(m_producerPartitionId).append(" ");
        sb.append("span [").append(DRLogSegmentId.getSequenceNumberFromDRId(getFirstDrId())).append("-");
        sb.append(DRLogSegmentId.getSequenceNumberFromDRId(getLastDrId()));
        sb.append(", size=").append(size()).append("]");
    }

    protected void toString(StringBuilder sb) {
        sb.append("lastSpUniqueId ").append(UniqueIdGenerator.toShortString(m_lastSpUniqueId)).append(" ");
        sb.append("lastMpUniqueId ").append(UniqueIdGenerator.toShortString(m_lastMpUniqueId)).append(" ");
        sb.append("from P").append(m_producerPartitionId).append(" ");
        for (Range<Long> entry : m_map.asRanges()) {
            sb.append("[").append(DRLogSegmentId.getDebugStringFromDRId(start(entry))).append(", ")
              .append(DRLogSegmentId.getDebugStringFromDRId(end(entry))).append("] ");
        }
    }

    public static void debugTraceTracker(VoltLogger log, Map<Integer, Map<Integer, DRSiteDrIdTracker>> trackers) {
        if (log.isTraceEnabled()) {
            for (Entry<Integer, Map<Integer, DRSiteDrIdTracker>> e1 : trackers.entrySet()) {
                for (Entry<Integer, DRSiteDrIdTracker> e2 : e1.getValue().entrySet()) {
                    log.trace("Tracker for Producer " + e1.getKey() + "'s PID " + e2.getKey() +
                            " contains " + e2.getValue().toShortString());
                }
            }
        }
    }
}
