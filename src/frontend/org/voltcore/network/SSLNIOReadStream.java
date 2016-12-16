package org.voltcore.network;

import org.voltcore.utils.DBBPool;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by mteixeira on 12/16/16.
 */
public class SSLNIOReadStream {

    private final Deque<DBBPool.BBContainer> m_readBBContainers = new ArrayDeque<>();
    private int m_totalAvailable = 0;
    private long m_bytesRead = 0;

    // pollLast

    // offerLast

    // need while loops

    // need the wrapping class


    int getBytes(ByteBuffer output) {
        int totalBytesCopied = 0;
        while (m_totalAvailable > 0 && output.hasRemaining()) {
            synchronized (m_readBBContainers) {
                if (m_readBBContainers.size() > 0) {

                }
            }
        }
        return totalBytesCopied;
    }

    final int read(ReadableByteChannel channel, int maxBytes, NetworkDBBPool pool) throws IOException {
        int bytesRead = 0;
        int lastRead = 1;
        DBBPool.BBContainer poolCont = null;
        try {
            while (bytesRead < maxBytes && lastRead > 0) {
                if (poolCont == null) {
                    synchronized (m_readBBContainers) {
                        if (!m_readBBContainers.isEmpty()) {
                            DBBPool.BBContainer last = m_readBBContainers.peekLast();
                            if (last.b().hasRemaining()) {
                                last = m_readBBContainers.pollLast();
                                poolCont = last;
                            }
                        }
                    }
                    if (poolCont == null) {
                        poolCont = pool.acquire();
                        poolCont.b().clear();
                    }
                }

                lastRead = channel.read(poolCont.b());

                // EOF, no data read
                if (lastRead < 0 && bytesRead == 0) {
                    return -1;
                }

                //Data read
                if (lastRead > 0) {
                    bytesRead += lastRead;
                    if (!poolCont.b().hasRemaining()) {
                        synchronized (m_readBBContainers) {
                            m_readBBContainers.addLast(poolCont);
                        }
                        poolCont = pool.acquire();
                        poolCont.b().clear();
                    }
                }
            }
        } finally {
            if (poolCont != null) {
                synchronized (m_readBBContainers) {
                    m_readBBContainers.addLast(poolCont);
                }
            }
            if (bytesRead > 0) {
                m_bytesRead += bytesRead;
                m_totalAvailable += bytesRead;
            }
        }

        return bytesRead;
    }


    private static class LockingBBContainer {
        public final DBBPool.BBContainer cont;
        public final Lock lock;

        public LockingBBContainer(DBBPool.BBContainer cont, Lock lock) {
            this.cont = cont;
            this.lock = lock;
        }
    }
}
