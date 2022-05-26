/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

package org.voltdb.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.voltdb.client.ConnectionUtil.DelayedExecutionThread;
import org.voltdb.client.ConnectionUtil.DelayedExecutionThread.State;

import com.google_voltpatches.common.util.concurrent.Uninterruptibles;

/**
 * Simple unit tests for {@link DelayedExecutionThread}
 */
public class TestConnectionUtilDelayedExecutionThread {
    /*
     * Test that runnable is only executed after delay time has passed
     */
    @Test(timeout = 1000)
    public void runAfterDelay() throws Exception {
        CountDownLatch cdl1 = new CountDownLatch(1);
        CountDownLatch cdl2 = new CountDownLatch(1);

        long delayMs = 30;
        long before = System.nanoTime();

        DelayedExecutionThread det = new DelayedExecutionThread(delayMs, TimeUnit.MILLISECONDS, () -> {
            cdl1.countDown();
            Uninterruptibles.awaitUninterruptibly(cdl2);
        });

        State state = det.state();
        assertTrue("State not not started: " + state, state == State.NOT_STARTED);

        det.start();

        // If false delay has already passed so test is not valid
        assumeTrue(System.nanoTime() - before < TimeUnit.MILLISECONDS.toNanos(delayMs));

        cdl1.await();
        assertTrue("Executed too early", System.nanoTime() - before >= TimeUnit.MILLISECONDS.toNanos(delayMs));

        state = det.state();
        assertTrue("State not running: " + state, state == State.RUNNING);

        cdl2.countDown();

        det.waitUntilDone();

        state = det.state();
        assertTrue("State not completed: " + state, state == State.COMPLETED);
    }

    /*
     * Test that a call to cancel causes the DelatedExecutionThread to exit immediately
     */
    @Test(timeout = 1000)
    public void cancelExitsThread() throws Exception {
        DelayedExecutionThread det = new DelayedExecutionThread(1, TimeUnit.HOURS, null);
        det.start();

        // Wait until the delay thread is waiting
        while (det.state() != State.WAITING) {
            Thread.yield();
        }

        det.cancel();

        State state = det.state();
        assertTrue("State not canceled: " + state, state == State.CANCELED);

        det.join();
    }

    /*
     * Test that calling cancel before start behaves correctly
     */
    @Test(timeout = 1000)
    public void cancleBeforeStart() throws Exception {
        boolean[] ran = { false };
        DelayedExecutionThread det = new DelayedExecutionThread(0, TimeUnit.NANOSECONDS, () -> ran[0] = true);
        det.cancel();

        State state = det.state();
        assertTrue("State not canceled: " + state, state == State.CANCELED);

        det.start();
        det.join();

        state = det.state();
        assertTrue("State not canceled: " + state, state == State.CANCELED);

        assertFalse("Runnable ran", ran[0]);
    }

    /*
     * Test that the cancel method blocks until the runnable completes
     */
    @Test(timeout = 1000)
    public void cancelBlockedByRun() throws Exception {
        CountDownLatch cdl1 = new CountDownLatch(1);
        CountDownLatch cdl2 = new CountDownLatch(1);

        long sleepTime = 15;
        boolean[] failed = { false };

        DelayedExecutionThread det = new DelayedExecutionThread(0, TimeUnit.NANOSECONDS, () -> {
            cdl1.countDown();
            try {
                cdl2.await();
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                failed[0] = true;
            }
        });
        det.start();

        cdl1.await();

        assertEquals(State.RUNNING, det.state());

        long before = System.nanoTime();
        cdl2.countDown();

        det.cancel();

        assertTrue(System.nanoTime() - before > TimeUnit.MILLISECONDS.toNanos(sleepTime));

        assertFalse(failed[0]);
    }
}
