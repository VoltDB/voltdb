/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

package txnIdSelfCheck;

import java.util.HashMap;
import java.util.Map.Entry;
import java.util.concurrent.Semaphore;

/**
 * A very naive rate limiter for txnId-selfcheck2.
 *
 * The rate limiter creates a semaphore for each type of transactions. The
 * semaphore is initialized with permits proportional to the given ratio of the
 * total permits. Each second, the owner can call resetPermits() to reset the
 * permits of each type back to the initial values.
 */
public class TxnId2RateLimiter {
    private final HashMap<Integer, Semaphore> m_permits =
        new HashMap<Integer, Semaphore>();
    private final HashMap<Integer, Integer> m_initialPermits =
        new HashMap<Integer, Integer>();
    private final long m_totalPermits;

    // last time permits were reset
    private long m_lastPermitResetTs = System.currentTimeMillis();

    public TxnId2RateLimiter(long totalPermits) {
        m_totalPermits = totalPermits;
    }

    /**
     * @param type type of invocation.
     * @param ratio ratio of permits in total permits for this type. 0 <= ratio <= 1.
     */
    public Semaphore addType(int type, double ratio) {
        Semaphore permits = new Semaphore((int) (m_totalPermits * ratio));
        m_permits.put(type, permits);
        m_initialPermits.put(type, permits.availablePermits());
        return permits;
    }

    public void resetPermits() {
        for (Entry<Integer, Semaphore> e : m_permits.entrySet()) {
            int type = e.getKey();
            Semaphore permits = e.getValue();

            permits.drainPermits();
            permits.release(m_initialPermits.get(type));
        }
    }

    /**
     * Updates the permits every second.
     *
     * Caller can call this method many times in a second, it will only update
     * the permits when a second has passed.
     */
    public void updateActivePermits(final long ts) {
        // Reset the permits roughly every second
        if (ts - m_lastPermitResetTs > 1000) {
            resetPermits();
            m_lastPermitResetTs = ts;
        }
    }
}
