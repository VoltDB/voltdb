/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

import java.util.Random;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import org.voltdb.ClientResponseImpl;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcedureCallback;

import txnIdSelfCheck.procedures.UpdateBaseProc;

public class ReadThread extends BenchmarkThread {

    Random r = new Random(0);
    long counter = 0;
    final Client client;
    final int threadCount;
    final int threadOffset;
    final AtomicBoolean m_shouldContinue = new AtomicBoolean(true);
    final AtomicBoolean m_needsBlock = new AtomicBoolean(false);
    final Semaphore txnsOutstanding = new Semaphore(100);
    final boolean allowInProcAdhoc;
    final float mpRatio;
    final Semaphore m_permits;

    public ReadThread(Client client, int threadCount, int threadOffset,
            boolean allowInProcAdhoc, float mpRatio, Semaphore permits)
    {
        setName("ReadThread");
        this.client = client;
        this.threadCount = threadCount;
        this.threadOffset = threadOffset;
        this.allowInProcAdhoc = allowInProcAdhoc;
        this.mpRatio = mpRatio;
        this.m_permits = permits;
    }

    void shutdown() {
        m_shouldContinue.set(false);
        this.interrupt();
    }

    class ReadCallback implements ProcedureCallback {
        @Override
        public void clientCallback(ClientResponse clientResponse) throws Exception {
            txnsOutstanding.release();
            if (clientResponse.getStatus() != ClientResponse.SUCCESS) {
                log.error("Non success in ProcCallback for ReadThread");
                log.error(((ClientResponseImpl)clientResponse).toJSONString());
                m_needsBlock.set(true);
                return;
            }
            Benchmark.txnCount.incrementAndGet();
            // validate the data
            try {
                VoltTable data = clientResponse.getResults()[0];
                UpdateBaseProc.validateCIDData(data, ReadThread.class.getName());
            }
            catch (Exception e) {
                hardStop("ReadThread got a bad response", e);
            }
        }
    }

    @Override
    public void run() {
        while (m_shouldContinue.get()) {

            // if a transaction callback has failed, sleep for 3 seconds
            // if not, connected, continue to sleep
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
            }

            // get a permit to send a transaction
            try {
                txnsOutstanding.acquire();
            } catch (InterruptedException e) {
                log.error("ReadThread interrupted while waiting for permit", e);
                return;
            }

            // 1/5 of all reads are MP
            boolean replicated = (counter % 100) < (this.mpRatio * 100.);
            // 1/23th of all SP reads are in-proc adhoc
            boolean inprocAdhoc = (counter % 23) == 0;
            counter++;
            String procName = replicated ? "ReadMP" : "ReadSP";
            if (inprocAdhoc && allowInProcAdhoc) procName += "InProcAdHoc";
            byte cid = (byte) (r.nextInt(threadCount) + threadOffset);

            // call a transaction
            try {
                m_permits.acquire();
                client.callProcedure(new ReadCallback(), procName, cid);
            }
            catch (NoConnectionsException e) {
                log.error("ReadThread got NoConnectionsException on proc call. Will sleep.");
                m_needsBlock.set(true);
            }
            catch (Exception e) {
                hardStop("ReadThread failed to run a procedure. Will exit.", e);
            }
        }
    }

}
