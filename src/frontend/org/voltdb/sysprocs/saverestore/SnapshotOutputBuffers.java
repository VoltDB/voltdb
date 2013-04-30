/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

package org.voltdb.sysprocs.saverestore;

import org.voltcore.utils.DBBPool;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * A helper class to encapsulate the serialization of snapshot output buffers
 */
public class SnapshotOutputBuffers {
    private final List<DBBPool.BBContainer> m_containers = new ArrayList<DBBPool.BBContainer>();

    public void addContainer(DBBPool.BBContainer container)
    {
        m_containers.add(container);
    }

    public List<DBBPool.BBContainer> getContainers()
    {
        return m_containers;
    }

    public byte[] toBytes()
    {
        ByteBuffer buf = ByteBuffer.allocate(4 + // buffer count
                                             (8 + 4 + 4) * m_containers.size()); // buffer info

        buf.putInt(m_containers.size());
        for (DBBPool.BBContainer container : m_containers) {
            buf.putLong(container.address);
            buf.putInt(container.b.position());
            buf.putInt(container.b.remaining());
        }

        return buf.array();
    }
}
