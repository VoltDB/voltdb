/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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
package org.voltdb.client.exampleutils;

import org.voltdb.client.ClientResponse;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ExecutionFuture implements Future<ClientResponse>
{
    private final CountDownLatch latch = new CountDownLatch(1);
    private final long timeout;
    private AtomicInteger status = new AtomicInteger(0);
    private static final int STATUS_RUNNING = 0;
    private static final int STATUS_SUCCESS = 1;
    private static final int STATUS_TIMEOUT = 2;
    private static final int STATUS_FAILURE = 3;
    private static final int STATUS_ABORTED = 4;
    private ClientResponse response = null;
    protected ExecutionFuture(long timeout)
    {
        this.timeout = timeout;
    }
    protected void set(ClientResponse response)
    {
        if (!status.compareAndSet(STATUS_RUNNING, STATUS_SUCCESS))
            return;
        this.response = response;
        this.latch.countDown();
    }
    @Override
    public boolean cancel(boolean mayInterruptIfRunning)
    {
        if (status.compareAndSet(STATUS_RUNNING, STATUS_ABORTED))
        {
            latch.countDown();
            return true;
        }
        return false;
    }
    @Override
    public ClientResponse get() throws InterruptedException, ExecutionException
    {
        try
        {
            return get(this.timeout, TimeUnit.MILLISECONDS);
        }
        catch (TimeoutException to)
        {
            status.compareAndSet(STATUS_RUNNING, STATUS_TIMEOUT);
            throw new ExecutionException(to);
        }
    }
    @Override
    public ClientResponse get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException
    {
        if(!latch.await(timeout, unit))
        {
            status.compareAndSet(STATUS_RUNNING, STATUS_TIMEOUT);
            throw new TimeoutException();
        }
        if (isCancelled())
        {
            throw new CancellationException();
        }
        else if (this.response.getStatus() != ClientResponse.SUCCESS)
        {
            status.compareAndSet(STATUS_RUNNING, STATUS_FAILURE);
            throw new ExecutionException(response.getException());
        }
        else
        {
            status.compareAndSet(STATUS_RUNNING, STATUS_SUCCESS);
            return this.response;
        }
    }
    @Override
    public boolean isCancelled()
    {
        return status.get() == STATUS_ABORTED;
    }
    @Override
    public boolean isDone()
    {
        return status.get() != STATUS_RUNNING;
    }
}
