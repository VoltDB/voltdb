/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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
import org.voltdb.client.ProcedureCallback;

import txnIdSelfCheck.procedures.UpdateBaseProc;

public class ReadThread extends Thread {

    Random r = new Random(0);
    long counter = 0;
    final Client client;
    final int threadCount;
    final int threadOffset;
    final AtomicBoolean m_shouldContinue = new AtomicBoolean(true);
    final Semaphore txnsOutstanding = new Semaphore(100);

    public ReadThread(Client client, int threadCount, int threadOffset) {
        this.client = client;
        this.threadCount = threadCount;
        this.threadOffset = threadOffset;
    }

    void shutdown() {
        m_shouldContinue.set(false);
    }

    class ReadCallback implements ProcedureCallback {
        @Override
        public void clientCallback(ClientResponse clientResponse) throws Exception {
            txnsOutstanding.release();
            if (clientResponse.getStatus() != ClientResponse.SUCCESS) {
                System.err.println("Non success in ProcCallback for AdHocMayhemThread");
                System.err.println(((ClientResponseImpl)clientResponse).toJSONString());
                return;
            }
            // validate the data
            try {
                VoltTable data = clientResponse.getResults()[0];
                UpdateBaseProc.validateCIDData(data, ReadThread.class.getName());
            }
            catch (Exception e) {
                System.err.println("ReadThread got a bad response");
                e.printStackTrace();
                System.exit(-1);
            }
        }
    }

    @Override
    public void run() {
        while (m_shouldContinue.get()) {
            try {
                txnsOutstanding.acquire();
            } catch (InterruptedException e) {
                System.err.println("ReadThread interrupted while waiting for permit");
                e.printStackTrace();
                return;
            }

            // 1/5 of all reads are MP
            boolean replicated = (counter++ % 5) == 0;
            String procName = replicated ? "ReadMP" : "ReadSP";
            byte cid = (byte) (r.nextInt(threadCount) + threadOffset);

            try {
                client.callProcedure(new ReadCallback(), procName, cid);
            }
            catch (Exception e) {
                System.err.println("ReadThread failed to run an AdHoc statement");
                e.printStackTrace();
                System.exit(-1);
            }
        }
    }

}
