/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;

import java.util.Random;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;


public class InvokeDroppedProcedureThread extends BenchmarkThread {

    Random r = new Random(8278923);
    long counter = 0;
    final long MAX_SIMPLE_UDF_EMPTY_TABLES = 1000;
    final Client client;
    final AtomicBoolean m_shouldContinue = new AtomicBoolean(true);
    final AtomicBoolean m_needsBlock = new AtomicBoolean(false);
    final Semaphore txnsOutstanding = new Semaphore(3);

    public InvokeDroppedProcedureThread(Client client) {
        setName("InvokeDroppedProcedureThread");
        this.client = client;
    }

    void shutdown() {
        m_shouldContinue.set(false);
        this.interrupt();
    }

    class InvokeDroppedCallback implements ProcedureCallback {
        protected boolean m_expectFailure;
        InvokeDroppedCallback() {
            this(false);
        }
        InvokeDroppedCallback(boolean expectFailure) {
            super();
            m_expectFailure = expectFailure;
        }
        @Override
        public void clientCallback(ClientResponse clientResponse) throws Exception {
            txnsOutstanding.release();
            log.info("InvokeDroppedProcedureThread response '" + clientResponse.getStatusString() + "' (" +  clientResponse.getStatus() + ")");
            // (! m_expectFailure) == "expect success".  So, if
            // we expect success but the result is not successful then we want
            // to crash.
            if ((! m_expectFailure) != (clientResponse.getStatus() == ClientResponse.SUCCESS)) {
                Benchmark.txnCount.incrementAndGet();
                hardStop(String.format("InvokeDroppedProcedureThread returned an unexpected status %d, %s failure.",
                                       clientResponse.getStatus(),
                                       (m_expectFailure ? "expected" : "did not expect")));
                //The procedure/udf may be dropped so we don't really care, just want to test the server with dropped procedure invocations in flight
            } else {
                validate(clientResponse);
            }
        }
        public void validate(ClientResponse cr) {
            // Do nothing here.
        }
    }

    String[] reasons = new String[] {
            "dropped Read procedure (should fail)",
            "dropped write procedure (should fail)",
            "UDF that throws a sql exception. (should fail)",
            "undefined UDF (should fail)",
            "invalid drop function (should fail)",
            "call simpleUDF2 (should succeed)",
            "call simpleUDF3 (should succeed)",
            "call simpleUDF4 (should succeed)",
            "call simpleUDF5 (should succeed)",
            "call simpleUDF6 (should succeed)",
            "call simpleUDF7 (should succeed)",
            "call simpleUDF8 (should succeed)",
            "call simpleUDF9 (should succeed)",
            "call simpleUDF10 (should succeed)",
    };

    private void callProcedure(Client client, InvokeDroppedCallback callback, String proc, Object... args) {
        boolean shouldfail = callback.m_expectFailure;
        ClientResponse response = null;
        try {
            response = TxnId2Utils.doProcCall(client, proc, args);
        }
        catch (ProcCallException e) {
            if (shouldfail)
                return;
            hardStop(e);
        }
        catch (Exception e) {
            hardStop(e);
        }
        try {
            callback.clientCallback(response);
        }
        catch (Exception e) {
            hardStop(e);
        }
    }

    @Override
    public void run() {
        // The function to compute t*10**N is at simpleUDF<N>
        // where N is between 2 and 10.  That is to say,
        // simpleUDF2 == (t)->((|t| + 2) * 10 ** 2), and
        // simpleUDF3 == (t)->((|t| + 2) * 10 ** 3) and so forth.
        final int SIMPLE_UDF_BASE = 5;
        final int MINIMUM_EXPONENT = 2;
        final int MAXIMUM_EXPONENT = 10;
        final int numberCases = SIMPLE_UDF_BASE + (MAXIMUM_EXPONENT - MINIMUM_EXPONENT + 1);

        // popluate a table with one row for this test
        try {
            TxnId2Utils.doProcCall(client, "PopulateDimension", (byte) 255);  // cids are 0-127 for ClientThread, 255 (-1) for us
        } catch (Exception e) {
            hardStop(e);
        }

        /*
         * Keep track of how many failures we have in a row.
         * Failures here means that a simpleUDF call produced
         * no rows.
         */
        final AtomicInteger failures = new AtomicInteger();
        while (m_shouldContinue.get()) {
            // if not, connected, sleep
            if (m_needsBlock.get()) {
                do {
                    try { Thread.sleep(3000); } catch (Exception e) {} // sleep for 3s
                    // bail on wakeup if we're supposed to bail
                    if (!m_shouldContinue.get()) {
                        return;
                    }
                }
                while (client.getConnectedHostList().size() == 0);
                m_needsBlock.set(false);
            } else {
                try { Thread.sleep(1); } catch (Exception e) {}
            }

            // get a permit to send a transaction
            try {
                txnsOutstanding.acquire();
            } catch (InterruptedException e) {
                hardStop("InvokeDroppedProcedureThread interrupted while waiting for permit. Will end.", e);
            }

            // call a transaction
            try {
                // We have SIMPLE_UDF_BASE cases for the non-simpleUDF calls,
                // Plus (MAXIMUM_EXPONENT - MINIMUM_EXPONENT + 1) exponents
                // for simpleUDF cases.
                final int caseNumber = r.nextInt(numberCases);
                log.info(String.format("InvokeDroppedProcedureThread running case %d: %s",
                                       caseNumber,
                                       reasons[caseNumber]));
                switch (caseNumber) {
                case 0:
                    // try to run a read procedure that has been droppped (or does not exist)
                    callProcedure(client, new InvokeDroppedCallback(true), "droppedRead", r.nextInt());
                    break;

                case 1:
                    // try to run a write procedure that has been dropped (or does not exist)
                    callProcedure(client, new InvokeDroppedCallback(true), "droppedWrite", r.nextInt());
                    break;

                case 2:
                    // run a udf that throws an exception
                    callProcedure(client, new InvokeDroppedCallback(true), "exceptionUDF");
                    break;

                case 3:
                    // run a statement using a function that is non-existent/dropped
                    try {
                        ClientResponse cr = TxnId2Utils.doAdHoc(client,
                                "select missingUDF(cid) FROM partitioned where cid=? order by cid, rid desc");
                    }
                    catch (ProcCallException e) {
                        log.info(e.getClientResponse().getStatus());
                        if (e.getClientResponse().getStatus() != ClientResponse.GRACEFUL_FAILURE)
                            hardStop(e);
                    }
                    break;

                case 4:
                    // try to drop a function which is used in the schema
                    // drop udf will put a planning error in the server log
                    try {
                        ClientResponse cr = TxnId2Utils.doAdHoc(client, "drop function excUDF;");
                        log.info(cr.getStatusString() + " (" + cr.getStatus() + ")");
                        if (cr.getStatus() == ClientResponse.SUCCESS)
                            hardStop("Should not succeed, the function is used in a stored procedure");
                    }
                    catch (Exception e) {
                        log.info("exception: ", e);
                    }
                    break;
                default:
                    int exponent = numberCases - SIMPLE_UDF_BASE;
                    /*
                     * The bound here is not really necessary, but we
                     * would not want overflow.
                     */
                    final long t = (long)r.nextInt(1000);
                    final long expected = getExpected(t, exponent);
                    if (2 <= exponent && exponent <= 10) {
                        callProcedure(client,
                                new InvokeDroppedCallback(false) {
                                    @Override
                                    public void validate(ClientResponse cr) {
                                        if (cr.getStatus() != ClientResponse.SUCCESS) {
                                            hardStop(String.format("simpleUDF(%d, %d) failed with status %d", t, exponent, cr.getStatus()));
                                        }
                                        VoltTable vt = cr.getResults()[0];
                                        // I think it's ok if there are no rows.  That
                                        // just means the table is not populated.  But if
                                        // it happens too often, that means there is some
                                        // problem with the test, and the test should fail.
                                        if (vt.advanceRow()) {
                                            long computed = vt.getLong(0);
                                            if (computed != expected) {
                                                hardStop(String.format("simpleUDF(%d, %d): expected %d, got %d.",
                                                        t, exponent, expected, computed));
                                            }
                                            failures.set(0);
                                        } else {
                                            int numFailures = failures.incrementAndGet();
                                            if (numFailures >= MAX_SIMPLE_UDF_EMPTY_TABLES) {
                                                hardStop("Too many empty tables for simpleUDF calls.");
                                            }
                                        }
                                    }
                                },
                                "SimpleUDF",
                                t,
                                exponent);
                    }
                }

                // don't flood the system with these
                Thread.sleep(r.nextInt(1000));
                txnsOutstanding.release();
            }
            catch (Exception e) {
                hardStop("InvokeDroppedProcedureThread failed to run client. Will exit.", e);
            }
        }
    }

    private long getExpected(long t, int exponent) {
        return Math.abs(t+2) * (long)Math.pow(10, exponent);
    }
}

