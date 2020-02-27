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

import org.voltcore.utils.DeferredSerialization;
import org.voltdb.catalog.Table;
import org.voltdb.serdes.EncodeFormat;
import org.voltdb.utils.SerializationHelper;

import com.google.common.base.Splitter;
import com.google_voltpatches.common.collect.ImmutableList;

/**
 * Class which holds all of the metadata which is persisted with export rows within a PBD. Primarily it holds the schema
 * of the rows
 */
public class PersistedMetadata implements DeferredSerialization {
    private static short LATEST_VERSION = 1;
    private final EncodeFormat m_format;
    private final List<String> m_keyColumns;
    private final ExportRowSchema m_schema;

    static PersistedMetadata deserialize(ByteBuffer buf) throws IOException {
        short version = buf.getShort();
        if (version != LATEST_VERSION) {
            throw new IOException("Unsupported serialization version: " + version);
        }
        EncodeFormat format = EncodeFormat.byId(buf.get());
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
        return new PersistedMetadata(format, keyColumns, ExportRowSchema.deserialize(buf));
    }

    public PersistedMetadata(Table table, int partitionId, long initialGenerationId, long generationId) {
        m_format = table.getIstopic() ? EncodeFormat.checkedValueOf(table.getTopicformat()) : EncodeFormat.INVALID;
        String keyColumns = table.getTopickeycolumnnames();
        if (keyColumns == null) {
            m_keyColumns = ImmutableList.of();
        } else {
            m_keyColumns = ImmutableList.copyOf(Splitter.on(',').trimResults().omitEmptyStrings().split(keyColumns));
            if (m_keyColumns.size() > 0xFF) {
                throw new IllegalArgumentException("Maximum number of key columns exceeded: " + m_keyColumns.size());
            }
        }
        m_schema = ExportRowSchema.create(table, partitionId, initialGenerationId, generationId);
    }

    private PersistedMetadata(EncodeFormat format, List<String> keyColumns, ExportRowSchema schema) {
        super();
        m_format = format;
        m_keyColumns = keyColumns;
        m_schema = schema;
    }

    /**
     * @return The configured {@link EncodeFormat} for this topic
     */
    public EncodeFormat getEncodingFormat() {
        return m_format;
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
        buf.put(m_format.getId());
        buf.put((byte) m_keyColumns.size());
        m_keyColumns.forEach(v -> SerializationHelper.writeString(v, buf));
        m_schema.serialize(buf);
    }

    @Override
    public void cancel() {
    }

    @Override
    public int getSerializedSize() throws IOException {
        // Version + format ID + keyColumnCount + serializedKeys size + schema size
        return Short.BYTES + Byte.BYTES + Byte.BYTES
                + m_keyColumns.stream().mapToInt(SerializationHelper::calculateSerializedSize).sum()
                + m_schema.getSerializedSize();
    }
}
