/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

package org.voltdb.client;

import java.util.concurrent.Semaphore;

/**
 * <p>A utility class that allows a client to queue a stored procedure invocation asynchronously and then poll
 * or join on the response. Useful when invoking multiple stored procedures synchronously
 * from a single thread. Queue each of the invocations asynchronously with a different <code>SyncCallback</code> and
 * then call {@link #waitForResponse} on each of the <code>SyncCallback</code>s to join on the responses.</p>
 *
 */
public final class SyncCallback extends AbstractProcedureArgumentCacher implements ProcedureCallback {
    private final Semaphore m_lock;
    private ClientResponse m_response;

    /**
     * Create a SyncCallback instance.
     */
    public SyncCallback() {
        m_response = null;
        m_lock = new Semaphore(1);
        m_lock.acquireUninterruptibly();
    }

    @Override
    public void clientCallback(ClientResponse clientResponse) {
        m_response = clientResponse;
        m_lock.release();
    }

    /**
     * <p>Non-blocking poll method that checks for the response to the invocation associated with this callback.
     * Call getResponse to retrieve the response or result() to retrieve the just the results.</p>
     *
     * @return True if the response is available, false otherwise
     */
    public boolean checkForResponse() {
        return m_lock.tryAcquire();
    }

    /**
     * <p>Retrieve the ClientResponse returned for this procedure invocation.</p>
     *
     * @return ClientResponse for this invocation
     */
    public ClientResponse getResponse() {
        return m_response;
    }

    /**
     * <p>Block until a response has been received for the invocation associated with this callback. Call getResponse
     * to retrieve the response or result() to retrieve the just the results.</p>
     *
     * @throws InterruptedException on interruption.
     */
    public void waitForResponse() throws InterruptedException {
        m_lock.acquire();
        m_lock.release();
    }

    /**
     * <p>Return the arguments provided with the procedure invocation.</p>
     * @return Object array containing procedure arguments
     */
    @Override
    public Object[] args() {
        return super.args();
    }
}
