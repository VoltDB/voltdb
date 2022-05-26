/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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
package org.voltdb.client.exampleutils;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.voltdb.client.ClientResponse;

/**
 * Provides a Future wrapper around an execution task's ClientResponse response object.
 *
 * @author Seb Coursol
 * @since 2.0
 */
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

    /**
     * Create a {@link Future} wrapper with the specified default timeout.
     *
     * @param timeout the default timeout for the execution call.
     */
    protected ExecutionFuture(long timeout)
    {
        this.timeout = timeout;
    }

    /**
     * Sets the result of the operation and flag the execution call as completed.
     *
     * @param response the execution call's response sent back by the database.
     */
    protected void set(ClientResponse response)
    {
        if (!status.compareAndSet(STATUS_RUNNING, STATUS_SUCCESS))
            return;
        this.response = response;
        this.latch.countDown();
    }

    /**
     * Attempts to cancel execution of this task (not supported - will always return <code>false</code>).
     *
     * @param mayInterruptIfRunning true if the thread executing this task should be interrupted; otherwise, in-progress tasks are allowed to complete.  This flag is ignored since VoltDB does not support cancellation of posted transaction requests.
     * @return <code>false</code> always: VoltDB does not support cancellation of posted transaction requests.
     */
    @Override
    public boolean cancel(boolean mayInterruptIfRunning)
    {
        return false;
    }

    /**
     * Waits if necessary for the computation to complete, and then retrieves its result.
     *
     * @return the computed result.
     * @throws CancellationException if the computation was cancelled.
     * @throws ExecutionException if the computation threw an exception.
     * @throws InterruptedException if the current thread was interrupted while waiting.
     */
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

    /**
     * Waits if necessary for at most the given time for the computation to complete, and then retrieves its result, if available.
     *
     * @param timeout the maximum time to wait.
     * @param unit the time unit of the timeout argument .
     * @return the computed result.
     * @throws CancellationException if the computation was cancelled.
     * @throws ExecutionException if the computation threw an exception.
     * @throws InterruptedException if the current thread was interrupted while waiting.
     * @throws TimeoutException if the wait timed out
     */
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
            throw new ExecutionException(new Exception(response.getStatusString()));
        }
        else
        {
            status.compareAndSet(STATUS_RUNNING, STATUS_SUCCESS);
            return this.response;
        }
    }

    /**
     * Returns <code>true</code> if this task was cancelled before it completed normally.
     *
     * @return <code>true</code> if this task was cancelled before it completed.
     */
    @Override
    public boolean isCancelled()
    {
        return status.get() == STATUS_ABORTED;
    }

    /**
     * Returns <code>true</code> if this task completed. Completion may be due to normal termination, an exception, or cancellation -- in all of these cases, this method will return true.
     *
     * @return <code>true</code> if this task completed.
     */
    @Override
    public boolean isDone()
    {
        return status.get() != STATUS_RUNNING;
    }
}
