/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltcore.network;

import java.util.ArrayDeque;

import org.voltcore.utils.DBBPool;
import org.voltcore.utils.DBBPool.BBContainer;

public class NetworkDBBPool {

    private final ArrayDeque<BBContainer> m_buffers = new ArrayDeque<BBContainer>();

    BBContainer acquire() {
       final BBContainer cont = m_buffers.poll();
       if (cont == null) {
           final BBContainer originContainer = DBBPool.allocateDirect(1024 * 32);
           return new BBContainer(originContainer.b, 0) {
                @Override
                public void discard() {
                    m_buffers.push(originContainer);
                }
           };
       }
       return new BBContainer(cont.b, 0) {
           @Override
           public void discard() {
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
