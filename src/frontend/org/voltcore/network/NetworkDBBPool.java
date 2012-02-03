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
