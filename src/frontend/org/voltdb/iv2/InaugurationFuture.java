/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

package org.voltdb.iv2;

import java.util.concurrent.atomic.AtomicBoolean;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;

import org.voltcore.utils.Pair;

// future that represents completion of transition to leader.
class InaugurationFuture implements Future<Pair<Boolean, Long>>
{
    private CountDownLatch m_doneLatch = new CountDownLatch(1);
    private AtomicBoolean m_cancelled = new AtomicBoolean(false);
    private ExecutionException m_exception = null;
    private Long m_maxTxnId = Long.MIN_VALUE;

    void setException(Exception e)
    {
        m_exception = new ExecutionException(e);
    }

    private Pair<Boolean, Long> result() throws ExecutionException
    {
        if (m_exception != null) {
            throw m_exception;
        }
        else if (isCancelled()) {
            return new Pair<Boolean, Long>(false, Long.MIN_VALUE);
        }
        return new Pair<Boolean, Long>(true, m_maxTxnId);

    }

    public void done(long maxTxnId)
    {
        m_maxTxnId = maxTxnId;
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
    public Pair<Boolean, Long> get() throws InterruptedException, ExecutionException
    {
        m_doneLatch.await();
        return result();
    }

    @Override
    public Pair<Boolean, Long> get(long timeout, TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException
    {
        assert false : "Inauguration future does not support timeout.";
        throw new UnsupportedOperationException("Inauguration future does not support timeout.");
    }
}
