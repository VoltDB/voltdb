/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltdb.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

public class TestMutex {

    @Test(timeout = 1_000)
    public void acquireWithResourceApi() {
        Mutex mutex = new Mutex();

        try (Mutex.Releaser r = mutex.acquire()) {
            assertTrue(mutex.isHeldByCurrentThread());
        }
        assertFalse(mutex.isHeldByCurrentThread());
    }

    @Test(timeout = 1_000)
    public void acquireInterruptiblyWithResourceApi() throws Exception {
        Mutex mutex = new Mutex();

        try (Mutex.Releaser r = mutex.acquireInterruptibly()) {
            assertTrue(mutex.isHeldByCurrentThread());
        }
        assertFalse(mutex.isHeldByCurrentThread());
    }

    @Test(timeout = 1_000)
    public void tryAcquireWithResourceApi() {
        Mutex mutex = new Mutex();

        try (Mutex.Releaser r = mutex.tryAcquire()) {
            assertTrue(mutex.isHeldByCurrentThread());
        }
        assertFalse(mutex.isHeldByCurrentThread());
    }

    @Test(timeout = 1_000)
    public void tryAcquireTimeoutWithResourceApi() throws Exception {
        Mutex mutex = new Mutex();

        try (Mutex.Releaser r = mutex.tryAcquire(1, TimeUnit.MINUTES)) {
            assertTrue(mutex.isHeldByCurrentThread());
        }
        assertFalse(mutex.isHeldByCurrentThread());
    }

    @Test(timeout = 1_000)
    public void canNotLockTwice() throws Exception {
        Mutex mutex = new Mutex();
        assertNotNull("Should have been able to acquire mutex", mutex.tryAcquire());

        try {
            mutex.acquire();
            fail("Should have thrown IllegalMonitorStateException");
        } catch (IllegalMonitorStateException e) {
        }

        try {
            mutex.acquireInterruptibly();
            fail("Should have thrown IllegalMonitorStateException");
        } catch (IllegalMonitorStateException e) {
        }

        try {
            mutex.tryAcquire();
            fail("Should have thrown IllegalMonitorStateException");
        } catch (IllegalMonitorStateException e) {
        }

        try {
            mutex.tryAcquire(10, TimeUnit.MILLISECONDS);
            fail("Should have thrown IllegalMonitorStateException");
        } catch (IllegalMonitorStateException e) {
        }

        mutex.release();
        assertFalse(mutex.isHeldByCurrentThread());

        assertNotNull("Should have been able to acquire mutex", mutex.tryAcquire());
        assertTrue(mutex.isHeldByCurrentThread());
        mutex.release();
    }

    @Test
    public void releaseThrowsIfNotHeld() {
        try {
            new Mutex().release();
            fail("Should have thrown IllegalMonitorStateException");
        } catch (IllegalMonitorStateException e) {
        }
    }

    @Test(timeout = 2_000)
    public void onlyOneThreadHoldsMutex() throws Exception {
        Mutex mutex = new Mutex();

        AtomicBoolean failed = new AtomicBoolean();
        AtomicBoolean lockHeld = new AtomicBoolean(true);
        AtomicInteger lockAcquiredCount = new AtomicInteger();

        mutex.acquire();
        assertTrue(mutex.isHeldByCurrentThread());

        Thread thread1 = new Thread() {
            @Override
            public void run() {
                try (Mutex.Releaser r = mutex.acquire()) {
                    lockAcquiredCount.getAndIncrement();
                    if (mutex.isHeldByCurrentThread()) {
                        failed.compareAndSet(false, lockHeld.get());
                    } else {
                        failed.set(true);
                    }
                } catch (Throwable t) {
                    t.printStackTrace();
                    failed.set(true);
                }
            }
        };
        thread1.setDaemon(true);

        Thread thread2 = new Thread() {
            @Override
            public void run() {
                try (Mutex.Releaser r = mutex.acquireInterruptibly()) {
                    lockAcquiredCount.getAndIncrement();
                    if (mutex.isHeldByCurrentThread()) {
                        failed.compareAndSet(false, lockHeld.get());
                    } else {
                        failed.set(true);
                    }
                } catch (InterruptedException e) {
                    failed.set(true);
                } catch (Throwable t) {
                    t.printStackTrace();
                    failed.set(true);
                }
            }
        };
        thread2.setDaemon(true);

        Thread thread3 = new Thread() {
            @Override
            public void run() {
                try (Mutex.Releaser r = mutex.tryAcquire(1, TimeUnit.SECONDS)) {
                    if (r != null) {
                        lockAcquiredCount.getAndIncrement();
                        if (mutex.isHeldByCurrentThread()) {
                            failed.compareAndSet(false, lockHeld.get());
                        } else {
                            failed.set(true);
                        }
                    } else {
                        failed.set(true);
                    }
                } catch (InterruptedException e) {
                    failed.set(true);
                } catch (Throwable t) {
                    t.printStackTrace();
                    failed.set(true);
                }
            }
        };
        thread3.setDaemon(true);

        thread1.start();
        thread2.start();
        thread3.start();

        do {
            Thread.yield();
        } while (mutex.getQueueLength() != 3);

        lockHeld.set(false);
        mutex.release();
        assertFalse(mutex.isHeldByCurrentThread());

        thread1.join(250);
        thread2.join(250);
        thread3.join(250);

        assertFalse(failed.get());
        assertEquals("Lock not acquired the expected number of times", 3, lockAcquiredCount.get());
    }

    @Test(timeout = 2_000)
    public void tryAcquireLockAlreadyHeld() throws Exception {
        Mutex mutex = new Mutex();
        boolean[] failed = { false };
        try (Mutex.Releaser r = mutex.acquire()) {
            Thread t = new Thread() {
                @Override
                public void run() {
                    try {
                        if (mutex.tryAcquire() != null || mutex.tryAcquire(10, TimeUnit.MICROSECONDS) != null) {
                            assertFalse(mutex.isHeldByCurrentThread());
                            failed[0] = true;
                        }
                    } catch (Throwable t) {
                        t.printStackTrace();
                        failed[0] = true;
                    }
                }
            };
            t.setDaemon(true);
            t.start();

            t.join();
        }

        assertFalse(failed[0]);
    }
}
