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
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.stream.Collectors;

import org.voltcore.utils.DBBPool.BBContainer;
import org.voltdb.common.Constants;
import org.voltdb.export.StreamBlock;
import org.voltdb.exportclient.PersistedMetadata;
import org.voltdb.messaging.VoltDbMessageFactory;
import org.voltdb.utils.SerializationHelper;

/**
 * Response to a {@link GapFillRequest}.
 * <p>
 * If {@link #isLastResponse()} returns {@code false} than a {@link GapFillContinue} message is expected in response to
 * this message.
 * <p>
 * If {@link #getMetadata()} returns {@code null} that means that the {@link StreamBlock}s in this response have the
 * same metadata as the blocks in the previous response
 */
public class GapFillResponse extends GapFillMessage {
    private static final byte s_hasMetadata = 1 << 0;
    private static final byte s_isLastResponse = 1 << 1;
    private static final byte s_isError = 1 << 2;

    private boolean m_lastResponse;
    private boolean m_isError = false;
    private PersistedMetadata m_metadata;
    private final Deque<StreamBlock> m_blocks = new ArrayDeque<>();
    private int m_totalBlocksSize = 0;
    private ByteBuffer m_serialized;

    public GapFillResponse() {
        super(VoltDbMessageFactory.E3_GAP_FILL_RESPONSE);
    }

    GapFillResponse(String streamName, int partitionId) {
        super(VoltDbMessageFactory.E3_GAP_FILL_RESPONSE, streamName, partitionId);
    }

    GapFillResponse setMetadata(PersistedMetadata metadata) {
        m_metadata = metadata;
        return this;
    }

    PersistedMetadata getMetadata() {
        return m_metadata;
    }

    void addBlock(StreamBlock block) {
        m_blocks.add(block);
        m_totalBlocksSize += Integer.BYTES + block.totalSize() + StreamBlock.HEADER_SIZE;
    }

    /**
     * @return List of {@link StreamBlock}s
     * @throws IllegalStateException if {@link #serialize()} has been called
     */
    List<StreamBlock> getStreamBlocks() {
        assert m_serialized == null : "StreamBlocks already serialized";
        return m_blocks.stream().collect(Collectors.toList());
    }

    /**
     * Remove the next {@link StreamBlock} from this instance. Once removed the caller is responsible for discarding the
     * {@link StremBlock}.
     *
     * @return the next {@link StreamBlock} or {@code null} if there are no more
     */
    StreamBlock poll() {
        return m_blocks.poll();
    }

    /**
     * @return {@code true} if this response does not have any {@link StreamBlock}s
     * @throws IllegalStateException if {@link #serialize()} has been called
     */
    boolean isEmpty() {
        assert m_serialized == null : "StreamBlocks already serialized";
        return m_blocks.isEmpty();
    }

    /**
     * @return total size of all stream blocks in this response
     */
    int getTotalBlocksSize() {
        return m_totalBlocksSize;
    }

    GapFillResponse setLastResponse() {
        m_lastResponse = true;
        return this;
    }

    boolean isLastResponse() {
        return m_lastResponse;
    }

    GapFillResponse setError() {
        m_lastResponse = true;
        m_isError = true;
        return this;
    }

    boolean isError() {
        return m_isError;
    }

    /**
     * Serialize this message to a buffer so that all {@link StreamBlock}s can be released. Once this is called
     * {@link #getStreamBlocks()} and {@link #isEmpty()} will no longer return valid responses
     *
     * @throws IOException If there was an error during serialization
     */
    void serialize() throws IOException {

        // calculate size: streamName + partitionId + flags + metadata + numberOfBlocks + blocks
        byte[] stringBytes = m_streamName.getBytes(Constants.UTF8ENCODING);
        m_serialized = ByteBuffer.allocate(Byte.BYTES + SerializationHelper.calculateSerializedSize(stringBytes)
                + Integer.BYTES + Byte.BYTES + (m_metadata == null ? 0 : m_metadata.getSerializedSize()) + Integer.BYTES
                + m_totalBlocksSize);

        // Serialize header metadata
        m_serialized.put(m_subject);
        SerializationHelper.writeVarbinary(stringBytes, m_serialized);
        m_serialized.putInt(m_partitionId);
        m_serialized.put(flags());
        if (m_metadata != null) {
            m_metadata.serialize(m_serialized);
        }

        // Serialize blocks now
        m_serialized.putInt(m_blocks.size());
        StreamBlock block;
        while ((block = m_blocks.poll()) != null) {
            BBContainer container = block.asBBContainer();
            try {
                ByteBuffer blockData = container.b();
                m_serialized.putInt(blockData.remaining());
                m_serialized.put(blockData);
            } finally {
                container.discard();
            }
        }

        m_serialized.flip();
    }

    /**
     * @return {@code true} if {@link #serialize()} has been invoked
     */
    boolean isSerialized() {
        return m_serialized == null;
    }

    ByteBuffer getSerialized() {
        return m_serialized.asReadOnlyBuffer();
    }

    void discard() {
        StreamBlock block;
        while ((block = m_blocks.poll()) != null) {
            block.discard();
        }
    }

    private byte flags() {
        byte flags = 0;
        if (m_metadata != null) {
            flags |= s_hasMetadata;
        }
        if (m_lastResponse) {
            flags |= s_isLastResponse;
        }
        if (m_isError) {
            flags |= s_isError;
        }
        return flags;
    }

    @Override
    public int getSerializedSize() {
        return m_serialized.remaining();
    }

    @Override
    protected void initFromBuffer(ByteBuffer buf) throws IOException {
        super.initFromBuffer(buf);
        byte flags = buf.get();
        m_lastResponse = (flags & s_isLastResponse) == s_isLastResponse;
        m_isError = (flags & s_isError) == s_isError;
        if ((flags & s_hasMetadata) == s_hasMetadata) {
            m_metadata = PersistedMetadata.deserialize(buf);
        } else {
            m_metadata = null;
        }

        int count = buf.getInt();
        int limit = buf.limit();

        m_totalBlocksSize = buf.remaining();

        for (int i = 0; i < count; ++i) {
            int size = buf.getInt();
            buf.limit(buf.position() + size);
            // Need to copy the data because StreamBlock needs a writable buffer and buf isn't
            ByteBuffer copy = ByteBuffer.allocate(size);
            copy.put(buf);
            copy.flip();
            m_blocks.add(StreamBlock.from(new HeapPBDEntry(copy)));
            buf.position(buf.limit());
            buf.limit(limit);
        }
    }

    @Override
    public void flattenToBuffer(ByteBuffer buf) throws IOException {
        buf.put(m_serialized.asReadOnlyBuffer());
    }

    @Override
    public String toString() {
        return "GapFillResponse [m_streamName=" + m_streamName + ", m_partitionId=" + m_partitionId
                + ", m_lastResponse=" + m_lastResponse + ", m_metadata=" + m_metadata + ", m_blocks=" + m_blocks
                + ", m_totalBlocksSize=" + m_totalBlocksSize + getSequenceNumbers() + "]";
    }

    private String getSequenceNumbers() {
        if (m_blocks.isEmpty()) {
            return "";
        }
        return ", startSequenceNumber=" + m_blocks.getFirst().startSequenceNumber() + ", lastSequenceNumber="
                + m_blocks.getLast().lastSequenceNumber();
    }
}
