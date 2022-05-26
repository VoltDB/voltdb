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

package org.voltdb.regressionsuites;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Can be used as a pipe output watcher that looks for a string.
 * Uses a timeout to fail if expected string not seen.
 * TODO: Extend for regex
 */
class OutputWatcher
{
    private final String m_searchString;
    private final long m_timeout;
    private CountDownLatch m_foundLatch = new CountDownLatch(1);

    OutputWatcher(String string, long timeout, TimeUnit unit) {
        m_searchString = string;
        m_timeout = unit.toMillis(timeout); // internally represent timeout as milliseconds
    }

    public void handleLine(String line)
    {
        //TODO: Regex
        if (line.contains(m_searchString)) {
            // below line is noop if already triggered
            m_foundLatch.countDown();
        }
    }

    boolean waitForString() {
        try {
            m_foundLatch.await(m_timeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            return false;
        }
        return true;
    }
}
