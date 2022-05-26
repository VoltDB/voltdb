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

package txnIdSelfCheck;

import org.voltdb.client.*;
import org.voltdb.client.ProcCallException;

import java.util.Random;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;


public class InvokeDroppedProcedureThread extends BenchmarkThread {

    Random r = new Random(8278923);
    long counter = 0;
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
        @Override
        public void clientCallback(ClientResponse clientResponse) throws Exception {
            txnsOutstanding.release();
            //log.info("InvokeDroppedProcedureThread response '" + clientResponse.getStatusString() + "' (" +  clientResponse.getStatus() + ")");
            if (clientResponse.getStatus() == ClientResponse.SUCCESS) {
                Benchmark.txnCount.incrementAndGet();
                hardStop("InvokeDroppedProcedureThread returned an unexpected status " + clientResponse.getStatus());
                //The procedure/udf may be dropped so we don't really care, just want to test the server with dropped procedure invocations in flight
            }
        }
    }

    @Override
    public void run() {

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
                int write = r.nextInt(4); // drop udf will put a planning error in the server log 5);
                log.info("InvokeDroppedProcedureThread running " + write);
                switch (write) {

                case 0:
                    // try to run a read procedure that has been droppped (or does not exist)
                    client.callProcedure(new InvokeDroppedCallback(), "droppedRead", r.nextInt());
                    break;

                case 1:
                    // try to run a write procedure that has been dropped (or does not exist)
                    client.callProcedure(new InvokeDroppedCallback(), "droppedWrite", r.nextInt());
                    break;

                case 2:
                    // run a udf that throws an exception
                    client.callProcedure(new InvokeDroppedCallback(), "exceptionUDF");
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
                    try {
                        ClientResponse cr = client.callProcedure("@AdHoc", "drop function add2Bigint;");
                        log.info(cr.getStatusString() + " (" + cr.getStatus() + ")");
                        if (cr.getStatus() == ClientResponse.SUCCESS)
                            hardStop("Should not succeed, the function is used in a stored procedure");
                    }
                    catch (Exception e) {
                        log.info("exception: ", e);
                    }
                    break;
                }

                // don't flood the system with these
                Thread.sleep(r.nextInt(1000));
                txnsOutstanding.release();
            }
            catch (NoConnectionsException e) {
                log.warn("InvokeDroppedProcedureThread got NoConnectionsException on proc call. Will sleep.");
                m_needsBlock.set(true);
            }
            catch (Exception e) {
                hardStop("InvokeDroppedProcedureThread failed to run client. Will exit.", e);
            }
        }
    }
}
