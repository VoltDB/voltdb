/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

/**
 * Can be used as a pipe output watcher that looks for a string.
 * Call isDone() in a loop to wait for string with a timeout.
 * Sleeps with a doubled duration between checks.
 * TODO: Extend for regex, better timeout logic, etc.
 */
class OutputStringWatcher implements OutputWatcher
{
    private final String m_string;
    private final long m_timeout;
    private boolean m_foundString = false;
    private final long m_startTime = System.currentTimeMillis();
    private long m_sleep = 0;

    OutputStringWatcher(String string, long timeout)
    {
        m_string = string;
        m_timeout = timeout;
    }

    @Override
    public void handleLine(String line)
    {
        //TODO: Regex
        if (line.contains(m_string)) {
            m_foundString  = true;
        }
    }

    private void sleep() throws InterruptedException
    {
        if (m_sleep > 0) {
            Thread.sleep(m_sleep);
            m_sleep *= 2;
        }
        else {
            m_sleep = 1000;
        }
    }

    boolean isDone()
    {
        try {
            sleep();
        }
        catch (InterruptedException e) {
            return true;
        }
        return (m_foundString || System.currentTimeMillis() - m_startTime > m_timeout);
    }

    boolean waitForString()
    {
        while (!isDone()) {;}
        return m_foundString;
    }
}