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

/**
 * Class which holds all of the metadata which is persisted with export rows within a PBD. Primarily it holds the schema
 * of the rows
 */
public class PersistedMetadata implements DeferredSerialization {
    private static short LATEST_VERSION = 1;
    private final ExportRowSchema m_schema;

    static PersistedMetadata deserialize(ByteBuffer buf) throws IOException {
        short version = buf.getShort();
        if (version != LATEST_VERSION) {
            throw new IOException("Unsupported serialization version: " + version);
        }
        return new PersistedMetadata(ExportRowSchema.deserialize(buf));
    }

    public PersistedMetadata(ExportRowSchema schema) {
        super();
        this.m_schema = schema;
    }

    public ExportRowSchema getSchema() {
        return m_schema;
    }

    @Override
    public void serialize(ByteBuffer buf) throws IOException {
        buf.putShort(LATEST_VERSION);
        m_schema.serialize(buf);
    }

    @Override
    public void cancel() {
    }

    @Override
    public int getSerializedSize() throws IOException {
        return Short.BYTES + m_schema.getSerializedSize();
    }
}
