/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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

package org.voltdb.client;
import java.util.concurrent.Semaphore;

/**
 * A utility class that allows a client to queue a stored procedure invocation asynchronously and then poll
 * or join on the response. Useful when invoking multiple stored procedures synchronously
 * from a single thread. Queue each of the invocations asynchronously with a different <code>SyncCallback</code> and
 * then call {@link #waitForResponse} on each of the <code>SyncCallback</code>s to join on the responses.
 *
 */
public final class SyncCallback extends AbstractProcedureArgumentCacher implements ProcedureCallback {
    private final Semaphore m_lock;
    private ClientResponse m_response;

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
     * Non-blocking poll method that checks for the response to the invocation associated with this callback.
     * Call getResponse to retrieve the response or result() to retrieve the just the results.
     * @return True if the response is available, false otherwise
     */
    public boolean checkForResponse() {
        return m_lock.tryAcquire();
    }

    /**
     * Retrieve the ClientResponse returned for this procedure invocation
     * @return ClientResponse for this invocation
     */
    public ClientResponse getResponse() {
        return m_response;
    }

    /**
     * Block until a response has been received for the invocation associated with this callback. Call getResponse
     * to retrieve the response or result() to retrieve the just the results.
     * @throws InterruptedException
     */
    public void waitForResponse() throws InterruptedException {
        m_lock.acquire();
        m_lock.release();
    }

    /**
     * Return the arguments provided with the procedure invocation
     * @return Object array containing procedure arguments
     */
    @Override
    public Object[] args() {
        return super.args();
    }
}
