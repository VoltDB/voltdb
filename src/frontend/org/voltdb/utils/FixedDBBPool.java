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
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class FixedDBBPool {
    /*
     * Keep track of the origin for each buffer so that they can be freed
     * individually as the each thread with ownership of the buffer discard them
     * post recovery completion.
     */
    protected final HashMap<DBBPool.BBContainer, DBBPool.BBContainer> m_bufferToOriginMap =
        new HashMap<DBBPool.BBContainer, DBBPool.BBContainer>();
    // Key is the size of the buffers in the correcponding queue
    protected final Map<Integer, LinkedBlockingQueue<DBBPool.BBContainer>> m_buffers =
        new HashMap<Integer, LinkedBlockingQueue<DBBPool.BBContainer>>();

    public FixedDBBPool()
    {
        if (!VoltDB.getLoadLibVOLTDB()) {
            throw new RuntimeException("Unable to load native library to allocate direct byte buffers");
        }

        EELibraryLoader.loadExecutionEngineLibrary(true);
    }

    public synchronized void allocate(int bufLenInBytes, int capacity)
    {
        LinkedBlockingQueue<DBBPool.BBContainer> bufQueue = m_buffers.get(bufLenInBytes);
        if (bufQueue == null) {
            bufQueue = new LinkedBlockingQueue<DBBPool.BBContainer>(capacity);
            m_buffers.put(bufLenInBytes, bufQueue);
        }

        final LinkedBlockingQueue<DBBPool.BBContainer> finalBufQueue = bufQueue;
        for (int ii = 0; ii < capacity; ii++) {
            final DBBPool.BBContainer origin = DBBPool.allocateDirect(bufLenInBytes);
            final long bufferAddress = DBBPool.getBufferAddress(origin.b);
            final DBBPool.BBContainer buffer = new DBBPool.BBContainer(origin.b, bufferAddress) {
                @Override
                public void discard() {
                    finalBufQueue.offer(this);
                }
            };
            m_bufferToOriginMap.put(buffer, origin);
            bufQueue.offer(buffer);
        }
    }

    public synchronized LinkedBlockingQueue<DBBPool.BBContainer> getQueue(int bufLenInBytes)
    {
        return m_buffers.get(bufLenInBytes);
    }

    /**
     * Discard all allocated buffers in the pool. Must call this after using the pool to free the
     * memory.
     *
     * This method is idempotent.
     */
    public synchronized void clear()
    {
        for (DBBPool.BBContainer originContainer : m_bufferToOriginMap.values()) {
            originContainer.discard();
        }
        m_bufferToOriginMap.clear();
        m_buffers.clear();
    }
}
