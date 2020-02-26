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

import org.voltcore.utils.DeferredSerialization;
import org.voltdb.catalog.Table;
import org.voltdb.serdes.EncodeFormat;

/**
 * Class which holds all of the metadata which is persisted with export rows within a PBD. Primarily it holds the schema
 * of the rows
 */
public class PersistedMetadata implements DeferredSerialization {
    private static short LATEST_VERSION = 1;
    private final ExportRowSchema m_schema;
    private final EncodeFormat m_format;

    static PersistedMetadata deserialize(ByteBuffer buf) throws IOException {
        short version = buf.getShort();
        if (version != LATEST_VERSION) {
            throw new IOException("Unsupported serialization version: " + version);
        }
        EncodeFormat format = EncodeFormat.byId(buf.get());
        return new PersistedMetadata(format, ExportRowSchema.deserialize(buf));
    }

    public PersistedMetadata(Table table, int partitionId, long initialGenerationId, long generationId) {
        m_format = table.getIstopic() ? EncodeFormat.checkedValueOf(table.getTopicformat()) : EncodeFormat.INVALID;
        m_schema = ExportRowSchema.create(table, partitionId, initialGenerationId, generationId);
    }

    private PersistedMetadata(EncodeFormat format, ExportRowSchema schema) {
        super();
        m_format = format;
        this.m_schema = schema;
    }

    /**
     * @return The configured {@link EncodeFormat} for this topic
     */
    public EncodeFormat getEncodingFormat() {
        return m_format;
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
        m_schema.serialize(buf);
    }

    @Override
    public void cancel() {
    }

    @Override
    public int getSerializedSize() throws IOException {
        // Version + format ID + schema size
        return Short.BYTES + Byte.BYTES + m_schema.getSerializedSize();
    }
}
