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
package org.voltdb.exportclient;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.voltcore.utils.DeferredSerialization;
import org.voltdb.catalog.Property;
import org.voltdb.catalog.Table;
import org.voltdb.catalog.Topic;
import org.voltdb.e3.topics.TopicProperties;
import org.voltdb.utils.SerializationHelper;

import com.google_voltpatches.common.collect.ImmutableMap;

/**
 * Class which holds all of the metadata which is persisted with export rows within a PBD. It holds the schema
 * of the rows, the encoding parameters for keys and values, and the key column names.
 */
public class PersistedMetadata implements DeferredSerialization {
    private static short LATEST_VERSION = 2;
    private final Map<String, String> m_topicProperties;
    private final ExportRowSchema m_schema;

    public static PersistedMetadata deserialize(ByteBuffer buf) throws IOException {
        short version = buf.getShort();
        if (version != LATEST_VERSION) {
            throw new IOException("Unsupported serialization version: " + version);
        }
        Map<String, String> topicProperties;
        int topicPropSize = buf.getShort();
        if (topicPropSize == 0) {
            topicProperties = ImmutableMap.of();
        } else {
            ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
            for (int i = 0; i < topicPropSize; ++i) {
                builder.put(SerializationHelper.getString(buf), SerializationHelper.getString(buf));
            }
            topicProperties = builder.build();
        }

        return new PersistedMetadata(topicProperties, ExportRowSchema.deserialize(buf));
    }

    public PersistedMetadata(Table table, int partitionId, long initialGenerationId, long generationId) {
        this(table, null, partitionId, initialGenerationId, generationId);
    }

    /**
     * Constructor
     *
     * @param table       Catalog {@link Table} which this is a source, or {@code null} if opaque topic
     * @param topic       Catalog {@link Topic} if the source is a topic, or {@code null}
     * @param partitionId
     * @param initialGenerationId
     * @param generationId
     */
    public PersistedMetadata(Table table, Topic topic, int partitionId, long initialGenerationId, long generationId) {
        assert table != null || topic != null;

        // Use 'topic' schema (i.e. an empty ExportRow) for opaque topics
        boolean useTopicSchema = table == null;
        if (topic == null) {
            m_topicProperties = ImmutableMap.of();
        }
        else {
            m_topicProperties = StreamSupport.stream(topic.getProperties().spliterator(), false)
                    .collect(Collectors.toMap(Property::getTypeName, Property::getValue));

            // Use 'topic' schema if non-opaque topic is using inline encoding.
            if (!topic.getIsopaque()) {
                useTopicSchema = TopicProperties.Key.TOPIC_STORE_ENCODED.get(m_topicProperties);
            }
        }
        m_schema = useTopicSchema ? ExportRowSchema.create(topic, partitionId, initialGenerationId, generationId)
                : ExportRowSchema.create(table, partitionId, initialGenerationId, generationId);
    }

    private PersistedMetadata(Map<String, String> topicProperties, ExportRowSchema schema) {
        super();
        m_topicProperties = topicProperties;
        m_schema = schema;
    }

    public Map<String, String> getTopicProperties() {
        return m_topicProperties;
    }

    /**
     * @return The {@link ExportRowSchema} associated with this metadata
     */
    public ExportRowSchema getSchema() {
        return m_schema;
    }

    @Override
    public void serialize(ByteBuffer buf) throws IOException {
        buf.putShort(LATEST_VERSION);
        if (m_topicProperties.isEmpty()) {
            buf.putShort((short) 0);
        } else {
            buf.putShort((short) m_topicProperties.size());
            m_topicProperties.forEach((k, v) -> {
                SerializationHelper.writeString(k, buf);
                SerializationHelper.writeString(v, buf);
            });
        }
        m_schema.serialize(buf);
    }

    @Override
    public void cancel() {
    }

    @Override
    public int getSerializedSize() throws IOException {
        // Version + topicPropertiesCount + topicProperties size + schema size
        return Short.BYTES * 2
                + m_topicProperties.entrySet().stream()
                        .mapToInt(e -> SerializationHelper.calculateSerializedSize(e.getKey())
                                + SerializationHelper.calculateSerializedSize(e.getValue()))
                        .sum()
                + m_schema.getSerializedSize();
    }

    @Override
    public String toString() {
        return "PersistedMetadata [m_topicProperties=" + m_topicProperties + "', m_schema=" + m_schema + "]";
    }

}
