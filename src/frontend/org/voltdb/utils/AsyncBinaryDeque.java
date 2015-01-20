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
package org.voltdb.utils;

import com.google_voltpatches.common.util.concurrent.ListenableFuture;
import com.google_voltpatches.common.util.concurrent.ListeningExecutorService;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltdb.utils.BinaryDeque.BinaryDequeTruncator;
import org.voltdb.utils.BinaryDeque.OutputContainerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.Semaphore;

/**
 * Async wrapper around a binary deque that does all actions in the provided executor service
 */
public class AsyncBinaryDeque {
    /*
     * Submit one of these to perform several operations agains the deque
     * in the IO thread of the deque
     */
    public interface OperateOnDeque<T> {
        public T operate(BinaryDeque deque) throws Exception;
    }

    private final BinaryDeque m_deque;
    private final ListeningExecutorService m_es;
    private final Semaphore m_taskLimit;

    public AsyncBinaryDeque(BinaryDeque deque, ListeningExecutorService es, Integer maxTasks)  {
        m_deque = deque;
        m_es = es;
        if (maxTasks != null) {
            m_taskLimit = new Semaphore(maxTasks);
        } else {
            m_taskLimit = null;
        }
    }

    private void releasePermit() {
        if (m_taskLimit != null) m_taskLimit.release();
    }

    private void acquirePermit() throws InterruptedException {
        if (m_taskLimit != null) m_taskLimit.acquire();
    }

    public ListenableFuture<?> offer(final BBContainer object) throws InterruptedException {
        acquirePermit();
        return m_es.submit(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                try {
                    m_deque.offer(object);
                    return null;
                } finally {
                    releasePermit();
                }
            }
        });
    }

    public ListenableFuture<?> push(final BBContainer objects[]) throws InterruptedException {
        acquirePermit();
        return m_es.submit(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                try {
                    m_deque.push(objects);
                    return null;
                } finally {
                    releasePermit();
                }
            }
        });
    }

    public ListenableFuture<BBContainer> poll(final OutputContainerFactory ocf) throws InterruptedException {
        acquirePermit();
        return m_es.submit(new Callable<BBContainer>() {
            @Override
            public BBContainer call() throws Exception {
                try {
                    return m_deque.poll(ocf);
                } finally {
                    releasePermit();
                }
            }
        });
    }

    public ListenableFuture<?> sync() throws InterruptedException {
        acquirePermit();
        return m_es.submit(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                try {
                    m_deque.sync();
                    return null;
                } finally {
                    releasePermit();
                }
            }
        });
    }

    public ListenableFuture<?> close()  throws InterruptedException {
        acquirePermit();
        return m_es.submit(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                try {
                    m_deque.close();
                    return null;
                } finally {
                    releasePermit();
                }
            }
        });
    }

    public ListenableFuture<Boolean> isEmpty() throws InterruptedException {
        acquirePermit();
        return m_es.submit(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                try {
                    return m_deque.isEmpty();
                } finally {
                    releasePermit();
                }
            }
        });
    }

    public ListenableFuture<Long> sizeInBytes() throws InterruptedException {
        acquirePermit();
        return m_es.submit(new Callable<Long>() {
            @Override
            public Long call() throws Exception {
                try {
                    return m_deque.sizeInBytes();
                } finally {
                    releasePermit();
                }
            }
        });
    }

    public ListenableFuture<?> closeAndDelete() throws InterruptedException {
        acquirePermit();
        return m_es.submit(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                try {
                    m_deque.closeAndDelete();
                    return null;
                } finally {
                    releasePermit();
                }
            }
        });
    }

    public ListenableFuture<?> parseAndTruncate(final BinaryDequeTruncator truncator) throws InterruptedException {
        acquirePermit();
        return m_es.submit(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                try {
                    m_deque.parseAndTruncate(truncator);
                    return null;
                } finally {
                    releasePermit();
                }
            }
        });
    }

    public <T> ListenableFuture<T> operateOnDeque(final OperateOnDeque<T> ood) throws InterruptedException {
        acquirePermit();
        return m_es.submit(new Callable<T>() {
            @Override
            public T call() throws Exception {
                try {
                    return ood.operate(m_deque);
                } finally {
                    releasePermit();
                }
            }
        });
    }
}
