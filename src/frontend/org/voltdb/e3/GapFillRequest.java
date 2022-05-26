/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
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

package org.voltdb.e3;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;

import org.voltdb.export.ExportSequenceNumberTracker;
import org.voltdb.messaging.VoltDbMessageFactory;

import com.google_voltpatches.common.collect.ImmutableList;
import com.google_voltpatches.common.collect.Range;

/**
 * A request for ranges of sequences from a specific stream and partition. The response to this is a
 * {@link GapFillResponse}.
 */
public class GapFillRequest extends GapFillMessage {
    private Collection<Range<Long>> m_ranges;

    public GapFillRequest() {
        super(VoltDbMessageFactory.E3_GAP_FILL_REQUEST);
    }

    /**
     * @param streamName  Name of stream
     * @param partitionId partition id ranges should be retrieved from
     * @param start       inclusive start sequence number
     * @param end         inclusive end sequence number
     */
    GapFillRequest(String streamName, int partitionId, long start, long end) {
        this(streamName, partitionId, ImmutableList.of(Range.closed(start, end)));
    }

    /**
     * @param streamName  Name of stream
     * @param partitionId partition id ranges should be retrieved from
     * @param ranges      {@link Collection} of {@link Range}s being requested
     */
    GapFillRequest(String streamName, int partitionId, Collection<Range<Long>> ranges) {
        super(VoltDbMessageFactory.E3_GAP_FILL_REQUEST, streamName, partitionId);
        m_ranges = ImmutableList.copyOf(ranges);
    }

    public Collection<Range<Long>> getRanges() {
        return m_ranges;
    }

    @Override
    public int getSerializedSize() {
        // super + rangeCount + ranges
        return super.getSerializedSize() + Integer.BYTES + m_ranges.size() * (Long.BYTES + Long.BYTES);
    }

    @Override
    protected void initFromBuffer(ByteBuffer buf) throws IOException {
        super.initFromBuffer(buf);
        int count = buf.getInt();
        ImmutableList.Builder<Range<Long>> builder = ImmutableList.builder();
        for (int i = 0; i < count; ++i) {
            builder.add(Range.closed(buf.getLong(), buf.getLong()));
        }
        m_ranges = builder.build();
    }

    @Override
    public void flattenToBuffer(ByteBuffer buf) throws IOException {
        super.flattenToBuffer(buf);
        buf.putInt(m_ranges.size());
        for (Range<Long> range : m_ranges) {
            buf.putLong(ExportSequenceNumberTracker.start(range));
            buf.putLong(ExportSequenceNumberTracker.end(range));
        }
    }
}
