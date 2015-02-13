/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

package org.voltcore.network;

import java.util.ArrayDeque;

import org.voltcore.utils.DBBPool;
import org.voltcore.utils.DBBPool.BBContainer;

public class NetworkDBBPool {

    private final ArrayDeque<BBContainer> m_buffers = new ArrayDeque<BBContainer>();
    private static final int LIMIT = Integer.getInteger("NETWORK_DBB_LIMIT", 512);
    private static final int SIZE = Integer.getInteger("NETWORK_DBB_SIZE", (1024 * 32));

    private final int m_numBuffers;
    private final int m_allocationSize;
    public NetworkDBBPool(int numBuffers) {
        m_numBuffers = numBuffers;
        m_allocationSize = SIZE;
    }

    NetworkDBBPool(int numBuffers, int allocSize) {
        m_numBuffers = numBuffers;
        m_allocationSize = allocSize;
    }

    public NetworkDBBPool() {
        m_numBuffers = LIMIT;
        m_allocationSize = SIZE;
    }

    BBContainer acquire() {
       final BBContainer cont = m_buffers.poll();
       if (cont == null) {
           final BBContainer originContainer = DBBPool.allocateDirect(m_allocationSize);
           return new BBContainer(originContainer.b()) {
                @Override
                public void discard() {
                    checkDoubleFree();
                    //If we had to allocate over the desired limit, start discarding
                    if (m_buffers.size() > m_numBuffers) {
                        originContainer.discard();
                        return;
                    }
                    m_buffers.push(originContainer);
                }
           };
       }
       return new BBContainer(cont.b()) {
           @Override
           public void discard() {
               checkDoubleFree();
               m_buffers.push(cont);
           }
       };
    }

    void clear() {
        BBContainer cont = null;
        while ((cont = m_buffers.poll()) != null) {
            cont.discard();
        }
    }

}
