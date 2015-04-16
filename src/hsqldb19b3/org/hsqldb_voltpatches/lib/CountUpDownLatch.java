/* Copyright (c) 2001-2014, The HSQL Development Group
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * Neither the name of the HSQL Development Group nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL HSQL DEVELOPMENT GROUP, HSQLDB.ORG,
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */


package org.hsqldb_voltpatches.lib;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;

/**
 * A variation on {@link java.util.CountDownLatch} to allow counting up.
 * <p>
 * Unlike the older version, which was a wrapper for
 * {@link java.util.concurrent.CountDownLatch}, this version directly mimics
 * CountDownLatch by delegating to {@link AbstractQueuedSynchronizer}.
 * <p>
 * The primary advantage of mimicry is that volatile variables are not used,
 * meaning that the underlying java libraries and java runtime are free to
 * fully and unrestrictedly capitalize upon whatever lock-free / wait-free
 * algorithms and hardware primitives are available (see:
 * http://www.ibm.com/developerworks/java/library/j-jtp11234/).
 * <p>
 * The secondary advantage is that by eliminating an inner CountDownLatch
 * instance that must be discarded and replaced every time the count increments
 * from zero, heap memory object burn rate is reduced.
 *
 *
 * @author Campbell Boucher-Burnet (boucherb@users dot sourceforge.net)
 */
public class CountUpDownLatch {

    /**
     * Synchronization control For CountUpDownLatch2.
     *
     * Uses AQS state property to represent count.
     */
    private static final class Sync extends AbstractQueuedSynchronizer {

        private static final long serialVersionUID = 1L;

        Sync(int count) {
            setState(count);
        }

        int getCount() {
            return getState();
        }

        /**
         * Queries if the state of this synchronizer permits it to be acquired
         * in the shared mode, and if so to acquire it.
         *
         * @param ignored
         * @return -1 on failure; 1 if acquisition in shared mode succeeded and
         *         subsequent shared-mode acquires might also succeed, in which
         *         case a subsequent waiting thread must check availability.
         */
        protected int tryAcquireShared(int ignored) {
            return getState() == 0 ? 1
                                   : -1;
        }

        /**
         * Updates the state of this object to the {@code requestedCount} value,
         * returning {@code true} on transition to zero.
         *
         * Note that negative {@code requestedCount} values are silently
         * converted to zero.
         *
         * @param requestedCount the value of the count property to be set
         * @return {@code true} if this release of shared mode may permit a
         *         waiting acquire (shared or exclusive) to succeed; and
         *         {@code false} otherwise
         */
        protected boolean tryReleaseShared(int requestedCount) {

            final int     newCount = Math.max(0, requestedCount);
            final boolean result   = (newCount == 0);

            for (;;) {
                if (compareAndSetState(getState(), newCount)) {
                    return result;
                }
            }
        }
    }

    private final Sync sync;

    public CountUpDownLatch() {
        this(0);
    }

    /**
     * Constructs a {@code CountUpDownLatch2} initialized with the given value.
     *
     * @param initialCount the initial value representing the number of times
     *        {@link #countDown} must be invoked before threads can pass
     *        through {@link #await}
     * @throws IllegalArgumentException if {@code initialCount} is negative
     */
    public CountUpDownLatch(int initialCount) {

        if (initialCount < 0) {
            throw new IllegalArgumentException("count < 0");
        }

        this.sync = new Sync(initialCount);
    }

    /**
     * Causes the current thread to wait until the latch has counted down to
     * zero, unless the thread is {@linkplain Thread#interrupt interrupted}.
     *
     * <p>If the current count is zero then this method returns immediately.
     *
     * <p>If the current count is greater than zero then the current
     * thread becomes disabled for thread scheduling purposes and lies
     * dormant until one of two things happen:
     * <ul>
     * <li>The count reaches zero due to invocations of the
     * {@link #countDown} method; or
     * <li>Some other thread {@linkplain Thread#interrupt interrupts}
     * the current thread.
     * </ul>
     *
     * <p>If the current thread:
     * <ul>
     * <li>has its interrupted status set on entry to this method; or
     * <li>is {@linkplain Thread#interrupt interrupted} while waiting,
     * </ul>
     * then {@link InterruptedException} is thrown and the current thread's
     * interrupted status is cleared.
     *
     * @throws InterruptedException if the current thread is interrupted
     *         while waiting
     */
    public void await() throws InterruptedException {
        sync.acquireSharedInterruptibly(1);
    }

    /**
     * Causes the current thread to wait until the latch has counted down to
     * zero, unless the thread is {@linkplain Thread#interrupt interrupted},
     * or the specified waiting time elapses.
     *
     * <p>If the current count is zero then this method returns immediately
     * with the value {@code true}.
     *
     * <p>If the current count is greater than zero then the current
     * thread becomes disabled for thread scheduling purposes and lies
     * dormant until one of three things happen:
     * <ul>
     * <li>The count reaches zero due to invocations of the
     * {@link #countDown} method; or
     * <li>Some other thread {@linkplain Thread#interrupt interrupts}
     * the current thread; or
     * <li>The specified waiting time elapses.
     * </ul>
     *
     * <p>If the count reaches zero then the method returns with the
     * value {@code true}.
     *
     * <p>If the current thread:
     * <ul>
     * <li>has its interrupted status set on entry to this method; or
     * <li>is {@linkplain Thread#interrupt interrupted} while waiting,
     * </ul>
     * then {@link InterruptedException} is thrown and the current thread's
     * interrupted status is cleared.
     *
     * <p>If the specified waiting time elapses then the value {@code false}
     * is returned.  If the time is less than or equal to zero, the method
     * will not wait at all.
     *
     * @param timeout the maximum time to wait
     * @param unit the time unit of the {@code timeout} argument
     * @return {@code true} if the count reached zero and {@code false}
     *         if the waiting time elapsed before the count reached zero
     * @throws InterruptedException if the current thread is interrupted
     *         while waiting
     */
    public boolean await(long timeout,
                         TimeUnit unit) throws InterruptedException {
        return sync.tryAcquireSharedNanos(1, unit.toNanos(timeout));
    }

    /**
     * Decrements the count of the latch, releasing all waiting threads if
     * the count reaches zero.
     *
     * <p>If the current count is greater than zero then it is decremented.
     * If the new count is zero then all waiting threads are re-enabled for
     * thread scheduling purposes.
     *
     * <p>If the current count equals zero then nothing happens.
     */
    public void countUp() {
        sync.releaseShared(sync.getCount() + 1);
    }

    /**
     * Decrements the count of the latch, releasing all waiting threads if
     * the count reaches zero.
     *
     * <p>If the current count is greater than zero then it is decremented.
     * If the new count is zero then all waiting threads are re-enabled for
     * thread scheduling purposes.
     *
     * <p>If the current count equals zero then nothing happens.
     */
    public void countDown() {
        sync.releaseShared(sync.getCount() - 1);
    }

    /**
     * Returns the current count.
     *
     * <p>This method is typically used only for debugging and testing purposes.
     *
     * @return the current count
     */
    public long getCount() {
        return sync.getCount();
    }

    public void setCount(int newCount) {
        sync.releaseShared(newCount);
    }

    /**
     * Returns a string identifying this latch, as well as its state.
     * The state, in brackets, includes the String {@code "Count ="}
     * followed by the current count.
     *
     * @return a string identifying this latch, as well as its state
     */
    public String toString() {
        return super.toString() + "[Count = " + sync.getCount() + "]";
    }
}
