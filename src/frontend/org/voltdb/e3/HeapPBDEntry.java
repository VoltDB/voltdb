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

import java.nio.ByteBuffer;

import org.voltdb.exportclient.PersistedMetadata;
import org.voltdb.utils.BinaryDequeReader;

class HeapPBDEntry implements BinaryDequeReader.Entry<PersistedMetadata> {
    private final ByteBuffer m_buffer;

    HeapPBDEntry(ByteBuffer buffer) {
        m_buffer = buffer;
    }

    @Override
    public PersistedMetadata getExtraHeader() {
        return null;
    }

    @Override
    public ByteBuffer getData() {
        return m_buffer.slice();
    }

    @Override
    public void release() {}

    @Override
    public void free() {}
}
