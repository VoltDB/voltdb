/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.iv2;

import java.util.concurrent.atomic.AtomicBoolean;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;

// future that represents completion of transition to leader.
class InaugurationFuture implements Future<Boolean>
{
    private CountDownLatch m_doneLatch = new CountDownLatch(1);
    private AtomicBoolean m_cancelled = new AtomicBoolean(false);
    private ExecutionException m_exception = null;

    void setException(Exception e)
    {
        m_exception = new ExecutionException(e);
    }

    private Boolean result() throws ExecutionException
    {
        if (m_exception != null) {
            throw m_exception;
        }
        else if (isCancelled()) {
            return false;
        }
        return true;

    }

    public void done()
    {
        m_doneLatch.countDown();
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning)
    {
        if (isDone()) {
            return false;
        }
        else {
            m_cancelled.set(true);
            m_doneLatch.countDown();
            return true;
        }
    }

    @Override
    public boolean isCancelled()
    {
        return m_cancelled.get();
    }

    @Override
    public boolean isDone()
    {
        return m_doneLatch.getCount() == 0;
    }

    @Override
    public Boolean get() throws InterruptedException, ExecutionException
    {
        m_doneLatch.await();
        return result();
    }

    @Override
    public Boolean get(long timeout, TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException
    {
        m_doneLatch.await(timeout, unit);
        return result();
    }
}


