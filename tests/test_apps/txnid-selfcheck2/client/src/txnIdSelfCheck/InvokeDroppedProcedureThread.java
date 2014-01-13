/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

import java.util.Date;
import java.util.Random;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;
import org.voltdb.ClientResponseImpl;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcedureCallback;

public class InvokeDroppedProcedureThread extends Thread {

    static Logger log = Logger.getLogger(InvokeDroppedProcedureThread.class);

    Random r = new Random(8278923);
    long counter = 0;
    final Client client;
    final AtomicBoolean m_shouldContinue = new AtomicBoolean(true);
    final AtomicBoolean m_needsBlock = new AtomicBoolean(false);
    final Semaphore txnsOutstanding = new Semaphore(100);

    public InvokeDroppedProcedureThread(Client client) {
        setName("InvokeDroppedProcedureThread");
        setDaemon(true);

        this.client = client;
    }

    void shutdown() {
        m_shouldContinue.set(false);
    }

    class InvokeDroppedCallback implements ProcedureCallback {
        @Override
        public void clientCallback(ClientResponse clientResponse) throws Exception {
            txnsOutstanding.release();
            //The procedure may be dropped so we don't really care, just want to test the server with dropped procedure invocations in flight
        }
    }

    @Override
    public void run() {
        try {
            while (m_shouldContinue.get()) {

                // if not, connected, sleep
                if (m_needsBlock.get()) {

                    Thread.sleep(3000); // sleep for 3s

                    while (client.getConnectedHostList().size() == 0);
                    m_needsBlock.set(false);

                } else {

                // call a transaction
                try {
                    boolean write = r.nextInt() % 2 == 0;
                    if (write) {
                        client.callProcedure(new InvokeDroppedCallback(), "droppedRead", r.nextInt());
                    } else {
                        client.callProcedure(new InvokeDroppedCallback(), "droppedWrite", r.nextInt());
                    }
                    // count txns
                    txnsOutstanding.acquire();
                }
                catch (NoConnectionsException e) {
                    log.error("InvokeDroppedProcedureThread got NoConnectionsException on proc call. Will sleep.");
                    m_needsBlock.set(true);
                }
                catch (Exception e) {
                    log.error("InvokeDroppedProcedureThread failed to run client. Will exit.", e);
                    Benchmark.printJStack();
                    System.exit(-1);
                }

                Thread.sleep(1);
                }
            }
        } catch (InterruptedException e) {
                log.error("InvokeDroppedProcedureThread interrupted while waiting for permit. Will end.", e);
                return;
        }
    }
}
