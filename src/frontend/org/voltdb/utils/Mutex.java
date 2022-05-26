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

package org.voltdb.utils;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.AbstractQueuedLongSynchronizer;

/**
 * A simple mutex which can only be locked and unlocked by the same thread once. If a thread attempts to lock the mutex
 * a second time {@link IllegalMonitorStateException} will be thrown.
 * <p>
 * This class returns a {@link Mutex.Releaser} from all acquire methods when the lock is successfully acquired.
 * {@link Mutex.Releaser} implements {@link AutoCloseable} so it can be used in a try with resources block. This
 * guarantees that the mutex will be released at the end of the try block automatically by java.
 *
 * <pre>
 * try (Mutex.Releaser r = mutex.acquire()) {
 *     ...
 * }
 * </pre>
 */
public final class Mutex {
    private final Sync m_sync = new Sync();
    private final Releaser m_releaser = new Releaser();

    /**
     * Lock this mutex blocking until the mutex can be acquired.
     *
     * @return {@link Mutex.Releaser} to release this mutex
     * @throws IllegalMonitorStateException if this mutex is already held by this thread
     */
    public Releaser acquire() {
        m_sync.acquire();
        return m_releaser;
    }

    /**
     * Acquire this mutex blocking until the mutex is acquired or the thread is interrupted
     *
     * @return {@link Mutex.Releaser} to release this mutex
     * @throws InterruptedException         If this thread was interrupted
     * @throws IllegalMonitorStateException if this mutex is already held by this thread
     */
    public Releaser acquireInterruptibly() throws InterruptedException {
        m_sync.acquireInterruptibly();
        return m_releaser;
    }

    /**
     * Test if the mutex can be acquired immediately and return whether or not it was successful
     *
     * @return {@link Mutex.Releaser} to release this mutex or {@code null} if the mutex was not acquired
     * @throws IllegalMonitorStateException if this mutex is already held by this thread
     */
    public Releaser tryAcquire() {
        return m_sync.tryAcquire() ? m_releaser : null;
    }

    /**
     * Try to acquire this mutex blocking until timeout was reached or the mutex was acquired.
     *
     * @return {@link Mutex.Releaser} to release this mutex or {@code null} if the mutex was not acquired
     * @throws InterruptedException         If this thread was interrupted
     * @throws IllegalMonitorStateException if this mutex is already held by this thread
     */
    public Releaser tryAcquire(long timeout, TimeUnit timeUnit) throws InterruptedException {
        return m_sync.tryAcquire(timeout, timeUnit) ? m_releaser : null;
    }

    /**
     * Release the mutex if held by this thread
     *
     * @throws IllegalMonitorStateException If this thread did not hold the mutex
     */
    public void release() throws IllegalMonitorStateException {
        m_sync.release();
    }

    /**
     * @return {@code true} if this thread holds the mutex
     */
    public boolean isHeldByCurrentThread() {
        return m_sync.isHeldByCurrentThread();
    }

    /**
     * @return {@code true} if this mutex is currently held by a thread
     */
    public boolean isHeld() {
        return m_sync.isHeld();
    }

    /**
     * @return An estimate of the number of threads waiting to acquire this mutex
     * @see AbstractQueuedLongSynchronizer#getQueueLength()
     */
    public int getQueueLength() {
        return m_sync.getQueueLength();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(super.toString()).append(": ");
        return m_sync.toString(sb).toString();
    }

    /**
     * Simple class that implements {@link AutoCloseable} to be able to release the mutex in a try with resources block
     */
    public final class Releaser implements AutoCloseable {
        /**
         * Release the mutex if held by this thread
         *
         * @throws IllegalMonitorStateException If this thread did not hold the mutex
         * @see Mutex#release()
         */
        @Override
        public void close() {
            release();
        }
    }

    /**
     * Synchronization implementation used by this mutex to guarantee non reentrent mutex holding
     */
    private static final class Sync extends AbstractQueuedLongSynchronizer {
        private static final long serialVersionUID = 1L;
        private static final long UNHELD = -1L;

        public Sync() {
            setState(UNHELD);
        }

        /**
         * Lock this mutex blocking until the mutex can be acquired.
         *
         * @throws IllegalMonitorStateException if this mutex is already held by this thread
         */
        public void acquire() {
            Thread thread = Thread.currentThread();
            acquire(thread.getId());
            setExclusiveOwnerThread(thread);
        }

        /**
         * Acquire this mutex blocking until the mutex is acquired or the thread is interrupted
         *
         * @throws InterruptedException         If this thread was interrupted
         * @throws IllegalMonitorStateException if this mutex is already held by this thread
         */
        public void acquireInterruptibly() throws InterruptedException {
            Thread thread = Thread.currentThread();
            acquireInterruptibly(thread.getId());
            setExclusiveOwnerThread(thread);
        }

        /**
         * Test if the mutex can be acquired immediately and return whether or not it was successful
         *
         * @return {@code true} if the mutex is now held by this thread
         * @throws IllegalMonitorStateException if this mutex is already held by this thread
         */
        public boolean tryAcquire() {
            Thread thread = Thread.currentThread();
            if (tryAcquire(thread.getId())) {
                setExclusiveOwnerThread(thread);
                return true;
            }
            return false;
        }

        /**
         * Try to acquire this mutex blocking until timeout was reached or the mutex was acquired.
         *
         * @return {@code true} if the mutex is now held by this thread
         * @throws InterruptedException         If this thread was interrupted
         * @throws IllegalMonitorStateException if this mutex is already held by this thread
         */
        public boolean tryAcquire(long timeout, TimeUnit timeUnit) throws InterruptedException {
            Thread thread = Thread.currentThread();
            if (tryAcquireNanos(thread.getId(), timeUnit.toNanos(timeout))) {
                setExclusiveOwnerThread(thread);
                return true;
            }
            return false;
        }

        /**
         * Release the mutex if held by this thread
         *
         * @throws IllegalMonitorStateException If this thread did not hold the mutex
         */
        public void release() throws IllegalMonitorStateException {
            release(Thread.currentThread().getId());
        }

        /**
         * @return {@code true} if this thread holds the mutex
         */
        public boolean isHeldByCurrentThread() {
            return Thread.currentThread() == getExclusiveOwnerThread();
        }

        /**
         * @return {@code true} if this mutex is currently held by a thread
         */
        public boolean isHeld() {
            return getState() != UNHELD;
        }

        public StringBuilder toString(StringBuilder sb) {
            Thread lockHolder = getExclusiveOwnerThread();
            if (lockHolder == null) {
                sb.append("unheld");
            } else {
                sb.append("held by thread ").append(lockHolder).append(" queue is ");
                if (!hasQueuedThreads()) {
                    sb.append("not ");
                }
                sb.append("empty");
            }
            return sb;
        }

        @Override
        protected boolean tryAcquire(long threadId) {
            if (!compareAndSetState(UNHELD, threadId)) {
                if (getState() == threadId) {
                    throw new IllegalMonitorStateException("Thread already holds this mutex");
                }
                return false;
            }
            return true;
        }

        @Override
        protected boolean tryRelease(long threadId) {
            if (getState() != threadId) {
                throw new IllegalMonitorStateException("Mutex not held by this thread");
            }
            // Clear owner before actually releasing the mutex
            setExclusiveOwnerThread(null);
            setState(UNHELD);
            return true;
        }
    }
}
