/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltdb.e3;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.junit.Rule;
import org.junit.Test;
import org.voltcore.messaging.VoltMessage;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltdb.VoltType;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Table;
import org.voltdb.catalog.Topic;
import org.voltdb.export.StreamBlock;
import org.voltdb.exportclient.PersistedMetadata;
import org.voltdb.messaging.VoltDbMessageFactory;
import org.voltdb.test.utils.RandomTestRule;

import com.google_voltpatches.common.collect.ImmutableList;
import com.google_voltpatches.common.collect.Range;

/**
 * Test that the messages which extend {@link GapFillMessage} all can be serialized and deserialized correctly
 */
public class TestGapFillMessages {
    @Rule
    public final RandomTestRule m_random = new RandomTestRule();
    private final VoltDbMessageFactory m_factory = new VoltDbMessageFactory();

    @Test
    public void gapFillRequest() throws Exception {
        validateDeserialzeRequest(new GapFillRequest("myTopic", 1, 100, 500));

        validateDeserialzeRequest(new GapFillRequest("otherTopic", 5, ImmutableList.of(Range.closed(50_000L, 55_000L),
                Range.closed(2_000L, 2_200L), Range.closed(1_000_000L, 1_200_000L))));

    }

    @Test
    public void gapFillResponse() throws Exception {
        validateDeserializedResponse(generateResponse());
        validateDeserializedResponse(generateResponse().setLastResponse());

        validateDeserializedResponse(generateResponse().setMetadata(
                new PersistedMetadata(generateTable(), m_random.nextInt(), m_random.nextInt(), m_random.nextInt()))
                .setLastResponse());

        GapFillResponse response = new GapFillResponse("myStream", m_random.nextInt()).setError();
        assertTrue(response.isError());
        assertTrue(response.isLastResponse());
        validateDeserializedResponse(response);
    }

    @Test
    public void gapFillContinue() throws Exception {
        GapFillContinue original = new GapFillContinue("someName", m_random.nextInt());
        GapFillContinue deserialized = serializeDeserialize(original);

        assertEquals(original.getStreamName(), deserialized.getStreamName());
        assertEquals(original.getPartitionId(), deserialized.getPartitionId());
    }

    private void validateDeserialzeRequest(GapFillRequest original) throws IOException {
        GapFillRequest deserialized = serializeDeserialize(original);

        assertEquals(original.getStreamName(), deserialized.getStreamName());
        assertEquals(original.getPartitionId(), deserialized.getPartitionId());
        assertEquals(original.getRanges(), deserialized.getRanges());
    }

    private Table generateTable() {
        CatalogMap<Table> tables = new Catalog().getClusters().add("cluster").getDatabases().add("database")
                .getTables();
        String typeName = "myTable";
        Table table = tables.add(typeName);
        Column c = table.getColumns().add("1");
        c.setIndex(0);
        c.setName("Column_1");
        c.setType(VoltType.INTEGER.getValue());
        c = table.getColumns().add("2");
        c.setIndex(1);
        c.setName("Column_2");
        c.setType(VoltType.STRING.getValue());
        c.setSize(4096);

        CatalogMap<Topic> topics = new Catalog().getClusters().add("cluster").getDatabases().add("database")
                .getTopics();
        topics.add(typeName);

        table.setTopicname(typeName);
        return table;
    }

    private GapFillResponse generateResponse() {
        GapFillResponse response = new GapFillResponse("myStream", m_random.nextInt());

        int blocks = m_random.nextInt(10);
        long startSeqId = m_random.nextInt();
        for (int i = 0; i < blocks; ++i) {
            ByteBuffer buf = ByteBuffer.allocate(m_random.nextInt(64_000) + 16_000);
            m_random.nextBytes(buf);
            buf.flip();
            int count = m_random.nextInt(50) + 10;
            response.addBlock(new StreamBlock(new HeapPBDEntry(buf), startSeqId, startSeqId + count, count,
                    m_random.nextLong()));
        }

        return response;
    }

    private void validateDeserializedResponse(GapFillResponse original) throws IOException {
        Map<StreamBlock, BBContainer> originalBlocks = original.getStreamBlocks().stream()
                .collect(Collectors.toMap(Function.identity(), StreamBlock::unreleasedContainer, (u, v) -> {
                    throw new IllegalStateException(String.format("Duplicate key %s", u));
                }, LinkedHashMap::new));
        original.serialize();
        GapFillResponse deserialized = serializeDeserialize(original);

        assertEquals(original.getStreamName(), deserialized.getStreamName());
        assertEquals(original.getPartitionId(), deserialized.getPartitionId());
        assertEquals(original.isLastResponse(), deserialized.isLastResponse());
        assertEquals(original.isError(), deserialized.isError());

        PersistedMetadata originalMetadata = original.getMetadata();
        PersistedMetadata deserializedMetadata = deserialized.getMetadata();
        assertEquals(originalMetadata == null, deserializedMetadata == null);
        if (originalMetadata != null) {
            assertEquals(originalMetadata.getTopicProperties(), deserializedMetadata.getTopicProperties());
            assertEquals(originalMetadata.getSchema().tableName, deserializedMetadata.getSchema().tableName);
            assertEquals(originalMetadata.getSchema().generation, deserializedMetadata.getSchema().generation);
            assertTrue(originalMetadata.getSchema().sameSchema(deserializedMetadata.getSchema()));
        }

        List<StreamBlock> deserializedBlocks = deserialized.getStreamBlocks();
        assertEquals(originalBlocks.size(), deserializedBlocks.size());

        int i = 0;
        for (Map.Entry<StreamBlock, BBContainer> entry : originalBlocks.entrySet()) {
            StreamBlock originalBlock = entry.getKey();
            StreamBlock deserializedBlock = deserializedBlocks.get(i++);
            assertEquals(originalBlock.startSequenceNumber(), deserializedBlock.startSequenceNumber());
            assertEquals(originalBlock.rowCount(), deserializedBlock.rowCount());
            assertEquals(originalBlock.committedSequenceNumber(), deserializedBlock.committedSequenceNumber());
            assertEquals(originalBlock.uniqueId(), deserializedBlock.uniqueId());
            assertEquals(originalBlock.totalSize(), deserializedBlock.totalSize());

            BBContainer deserialisedBbc = deserializedBlock.unreleasedContainer();
            assertEquals(entry.getValue().b(), deserialisedBbc.b());
            entry.getValue().discard();
            deserialisedBbc.discard();
        }

        deserialized.discard();
    }

    private <T extends VoltMessage> T serializeDeserialize(T original) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(original.getSerializedSize());
        original.flattenToBuffer(buf);
        assertFalse(buf.hasRemaining());
        buf.flip();

        @SuppressWarnings("unchecked")
        T deserialized = (T) m_factory.createMessageFromBuffer(buf, 1);
        return deserialized;
    }
}
