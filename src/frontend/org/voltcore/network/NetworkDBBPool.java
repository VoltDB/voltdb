/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

import org.voltcore.utils.DBBPool;
import org.voltcore.utils.DBBPool.BBContainer;

public class NetworkDBBPool {

    private static final int LIMIT = Integer.getInteger("NETWORK_DBB_LIMIT", 512);
    private static final int SIZE = Integer.getInteger("NETWORK_DBB_SIZE", (1024 * 32));

    private final Queue<BBContainer> m_buffers;
    private final int m_allocationSize;

    public NetworkDBBPool(int numBuffers) {
        this(numBuffers, SIZE);
    }

    NetworkDBBPool(int numBuffers, int allocSize) {
        m_buffers = new ArrayBlockingQueue<>(numBuffers);
        m_allocationSize = allocSize;
    }

    public NetworkDBBPool() {
        this(LIMIT, SIZE);
    }

    BBContainer acquire() {
        BBContainer cont = m_buffers.poll();

        if (cont == null) {
            cont = DBBPool.allocateDirect(m_allocationSize);
        }

        return new CachedContainer(cont);
    }

    void clear() {
        BBContainer cont = null;
        while ((cont = m_buffers.poll()) != null) {
            cont.discard();
        }
    }

    private final class CachedContainer extends BBContainer {
        private final BBContainer m_original;

        CachedContainer(BBContainer container) {
            super(container.b());
            m_original = container;
        }

        @Override
        public void discard() {
            checkDoubleFree();
            if (!m_buffers.offer(m_original)) {
                m_original.discard();
            }
        }
    }
}
