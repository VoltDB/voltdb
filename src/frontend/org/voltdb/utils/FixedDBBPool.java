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

package org.voltdb.utils;

import org.voltcore.utils.DBBPool;
import org.voltdb.EELibraryLoader;
import org.voltdb.VoltDB;

import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class FixedDBBPool {
    /*
     * Keep track of the origin for each buffer so that they can be freed
     * individually as the each thread with ownership of the buffer discard them
     * post recovery completion.
     */
    protected final HashMap<DBBPool.BBContainer, DBBPool.BBContainer> m_bufferToOriginMap =
        new HashMap<DBBPool.BBContainer, DBBPool.BBContainer>();
    protected final LinkedBlockingQueue<DBBPool.BBContainer> m_buffers =
        new LinkedBlockingQueue<DBBPool.BBContainer>();

    public FixedDBBPool(int bufLenInBytes, int capacity)
    {
        if (!VoltDB.getLoadLibVOLTDB()) {
            throw new RuntimeException("Unable to load native library to allocate direct byte buffers");
        }

        EELibraryLoader.loadExecutionEngineLibrary(true);
        initializePool(bufLenInBytes, capacity);
    }

    private void initializePool(int bufLenInBytes, int capacity)
    {
        for (int ii = 0; ii < capacity; ii++) {
            final DBBPool.BBContainer origin = DBBPool.allocateDirect(bufLenInBytes);
            final long bufferAddress = DBBPool.getBufferAddress(origin.b);
            final DBBPool.BBContainer buffer = new DBBPool.BBContainer(origin.b, bufferAddress) {
                @Override
                public void discard() {
                    m_buffers.offer(this);
                }
            };
            m_bufferToOriginMap.put(buffer, origin);
            m_buffers.offer(buffer);
        }
    }

    public LinkedBlockingQueue<DBBPool.BBContainer> getQueue()
    {
        return m_buffers;
    }

    public void clear()
    {
        for (DBBPool.BBContainer originContainer : m_bufferToOriginMap.values()) {
            originContainer.discard();
        }
        m_bufferToOriginMap.clear();
    }
}
