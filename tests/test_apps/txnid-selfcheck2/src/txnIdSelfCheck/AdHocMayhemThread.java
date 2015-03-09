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

import java.util.Date;
import java.util.Random;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import org.voltdb.ClientResponseImpl;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcedureCallback;

public class AdHocMayhemThread extends BenchmarkThread {

    Random r = new Random(0);
    long counter = 0;
    final Client client;
    final float mpRatio;
    final AtomicBoolean m_shouldContinue = new AtomicBoolean(true);
    final AtomicBoolean m_needsBlock = new AtomicBoolean(false);
    final Semaphore txnsOutstanding = new Semaphore(100);
    final Semaphore m_permits;

    public AdHocMayhemThread(Client client, float mpRatio, Semaphore permits) {
        setName("AdHocMayhemThread");
        this.client = client;
        this.mpRatio = mpRatio;
        this.m_permits = permits;
    }

    private String nextAdHoc() {

        // mpRatio % of all adhocs are MP
        boolean replicated = (counter % 100) < (this.mpRatio * 100.);
        boolean batched = (counter % 11) == 0 && this.mpRatio > 0.0;

        // batched statements can go rw, wr or ww
        long rwMix = counter % 3;

        String sql = "";

        if (batched) {
            if (rwMix == 0) {
                sql += nextWriteAdHocStmt(replicated);
            }
            if (rwMix == 1) {
                sql += nextReadAdHocStmt(replicated);
            }
        }

        sql += nextWriteAdHocStmt(replicated);

        if (batched) {
            if (rwMix == 2) {
                sql += nextReadAdHocStmt(replicated);
            }
        }
        counter++;

        return sql;
    }

    private String nextWriteAdHocStmt(boolean replicated) {
        String sql = "update";
        sql += replicated ? " adhocr " : " adhocp";
        sql += " set";
        sql += " ts = " + new Date().getTime() + ",";
        sql += " inc = inc + 1,";
        sql += " jmp = jmp + " + r.nextInt(10);
        if (!replicated) {
            sql += " where id = " + r.nextInt(10);
        }
        sql += ";";

        return sql;
    }

    private String nextReadAdHocStmt(boolean replicated) {
        String sql = "select * from";
        sql += replicated ? " adhocr " : " adhocp";
        if (!replicated) {
            sql += " where id = " + r.nextInt(10);
        }
        sql += " order by id limit 1;";

        return sql;
    }

    void shutdown() {
        m_shouldContinue.set(false);
        this.interrupt();
    }

    class AdHocCallback implements ProcedureCallback {
        @Override
        public void clientCallback(ClientResponse clientResponse) throws Exception {
            txnsOutstanding.release();
            if (clientResponse.getStatus() != ClientResponse.SUCCESS) {
                log.warn("Non success in ProcCallback for AdHocMayhemThread. Will sleep.");
                log.warn(((ClientResponseImpl)clientResponse).toJSONString());
                m_needsBlock.set(true);
            }
            else
                Benchmark.txnCount.incrementAndGet();
        }
    }

    @Override
    public void run() {
        try {
            client.callProcedure("SetupAdHocTables");
        } catch (Exception e) {
            hardStop("SetupAdHocTables failed in AdHocMayhemThread. Will exit.", e);
        }

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
                log.error("AdHocMayhemThread interrupted while waiting for permit. Will end AdHoc work.", e);
                return;
            }

            // call a transaction
            String sql = nextAdHoc();
            try {
                m_permits.acquire();
                client.callProcedure(new AdHocCallback(), "@AdHoc", sql);
            }
            catch (NoConnectionsException e) {
                log.error("AdHocMayhemThread got NoConnectionsException on proc call. Will sleep.");
                m_needsBlock.set(true);
            }
            catch (Exception e) {
                hardStop("AdHocMayhemThread failed to run an AdHoc statement. Will exit.", e);
            }
        }
    }
}
