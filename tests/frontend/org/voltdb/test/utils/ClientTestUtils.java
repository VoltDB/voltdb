/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
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
package org.voltdb.test.utils;

import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;

public class ClientTestUtils {
    /**
     * Call a procedure one time for each value returned by {@code arguments}. If any invocation fails an
     * {@link org.junit.Assert#fail()} will be called with results of the failure. If no failures occur this method will
     * return once the result of all invocations has been received.
     *
     * @param client        to use to make invocations
     * @param procedureName of the procedure to invoke
     * @param arguments     {@link Iterator} for the arguments to be used for each invocation
     * @throws Exception When an error occurrs
     */
    public static void callProcedureRepeatedly(Client client, String procedureName, Iterator<Object[]> arguments)
            throws Exception {
        ResultCollector callback = new ResultCollector();

        while (arguments.hasNext()) {
            callback.sendingRequest();
            client.callProcedure(callback, procedureName, arguments.next());
        }

        callback.waitForAllOrError();
    }

    /**
     * {@link ProcedureCallback} which is used to determine if all results were successful or not.
     */
    public static class ResultCollector implements ProcedureCallback {
        AtomicInteger outstandingResponses = new AtomicInteger();
        ClientResponse errorResponse = null;

        @Override
        public void clientCallback(ClientResponse clientResponse) {
            if (clientResponse.getStatus() != ClientResponse.SUCCESS) {
                synchronized (this) {
                    if (errorResponse == null) {
                        errorResponse = clientResponse;
                        notifyAll();
                    }
                }
            }
            if (outstandingResponses.decrementAndGet() == 0) {
                synchronized (this) {
                    notifyAll();
                }
            }
        }

        void sendingRequest() {
            outstandingResponses.getAndIncrement();
        }

        synchronized void waitForAllOrError() throws Exception {
            while (errorResponse == null && outstandingResponses.get() != 0) {
                wait();
            }

            if (errorResponse != null) {
                fail(errorResponse.getStatusString() + '\n' + Arrays.toString(errorResponse.getResults()));
            }
        }
    }
}
