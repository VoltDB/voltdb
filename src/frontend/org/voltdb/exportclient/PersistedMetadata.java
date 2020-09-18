/* This file is part of VoltDB.
 * Copyright (C) 2020 VoltDB Inc.
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
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.voltcore.utils.DeferredSerialization;
import org.voltdb.catalog.Table;
import org.voltdb.catalog.Topic;
import org.voltdb.serdes.EncodeConfiguration;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.SerializationHelper;

import com.google_voltpatches.common.collect.ImmutableList;

/**
 * Class which holds all of the metadata which is persisted with export rows within a PBD. It holds the schema
 * of the rows, the encoding parameters for keys and values, and the key column names.
 */
public class PersistedMetadata implements DeferredSerialization {
    private static short LATEST_VERSION = 2;
    private final EncodeConfiguration m_keyFormat;
    private final EncodeConfiguration m_valueFormat;
    private final List<String> m_keyColumns;
    private final ExportRowSchema m_schema;

    public static PersistedMetadata deserialize(ByteBuffer buf) throws IOException {
        short version = buf.getShort();
        if (version != LATEST_VERSION) {
            throw new IOException("Unsupported serialization version: " + version);
        }
        EncodeConfiguration keyFormat = EncodeConfiguration.deserialize(buf);
        EncodeConfiguration valueFormat = EncodeConfiguration.deserialize(buf);
        List<String> keyColumns;
        // keyCount is stored as unsigned byte
        int keyCount = (buf.get() & 0xFF);
        if (keyCount > 0) {
            ImmutableList.Builder<String> builder = ImmutableList.builder();
            for (int i = 0; i < keyCount; ++i) {
                builder.add(SerializationHelper.getString(buf));
            }
            keyColumns = builder.build();
        } else {
            keyColumns = ImmutableList.of();
        }
        return new PersistedMetadata(keyFormat, valueFormat, keyColumns, ExportRowSchema.deserialize(buf));
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
        if (topic == null) {
            // FIXME: done in DDL validation?
            m_keyFormat = m_valueFormat = new EncodeConfiguration();
            m_keyColumns = ImmutableList.of();
        }
        else {
            m_keyFormat = new EncodeConfiguration(topic.getIsopaque(), topic.getKeyformatname(), topic.getKeyformatproperties());
            m_valueFormat = new EncodeConfiguration(topic.getIsopaque(), topic.getValueformatname(), topic.getValueformatproperties());

            String keyColumns = topic.getKeycolumnnames();
            if (StringUtils.isBlank(keyColumns)) {
                m_keyColumns = ImmutableList.of();
            } else {
                m_keyColumns = ImmutableList.copyOf(CatalogUtil.splitOnCommas(keyColumns));
                if (m_keyColumns.size() > 0xFF) {
                    throw new IllegalArgumentException("Maximum number of key columns exceeded: " + m_keyColumns.size());
                }
            }
        }
        m_schema = table != null ? ExportRowSchema.create(table, partitionId, initialGenerationId, generationId)
                : ExportRowSchema.create(topic, partitionId, initialGenerationId, generationId);
    }

    private PersistedMetadata(EncodeConfiguration keyFormat, EncodeConfiguration valueFormat, List<String> keyColumns, ExportRowSchema schema) {
        super();
        m_keyFormat = keyFormat;
        m_valueFormat = valueFormat;
        m_keyColumns = keyColumns;
        m_schema = schema;
    }

    /**
     * @return The configured {@link EncodeConfiguration} for this topic
     * FIXME: for now return the encoding format of the value
     */
    public EncodeConfiguration getEncodingFormat() {
        return getValueFormat();
    }

    /**
     * @return the encoding configuration for keys
     */
    public EncodeConfiguration getKeyFormat() {
        return m_keyFormat;
    }

    /**
     * @return the encoding configuration for values
     */
    public EncodeConfiguration getValueFormat() {
        return m_valueFormat;
    }

    /**
     * @return The list of columns which should be used as the key when formatted as a topic
     */
    public List<String> getKeyColumns() {
        return m_keyColumns;
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
        m_keyFormat.serialize(buf);
        m_valueFormat.serialize(buf);
        buf.put((byte) m_keyColumns.size());
        m_keyColumns.forEach(v -> SerializationHelper.writeString(v, buf));
        m_schema.serialize(buf);
    }

    @Override
    public void cancel() {
    }

    @Override
    public int getSerializedSize() throws IOException {
        // Version + key format size + value format size + keyColumnCount + serializedKeys size + schema size
        return Short.BYTES + m_keyFormat.getSerializedSize() + m_valueFormat.getSerializedSize() + Byte.BYTES
                + m_keyColumns.stream().mapToInt(SerializationHelper::calculateSerializedSize).sum()
                + m_schema.getSerializedSize();
    }

    @Override
    public String toString() {
        return "PersistedMetadata [m_keyFormat=" + m_keyFormat + ", m_valueFormat" + m_valueFormat
                + ", m_keyColumns='" + m_keyColumns + "', m_schema="
                + m_schema + "]";
    }

}
