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

import java.util.Date;
import java.util.Random;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import org.voltdb.ClientResponseImpl;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;

public class AdHocMayhemThread extends Thread {

    Random r = new Random(0);
    long counter = 0;
    final Client client;
    final AtomicBoolean m_shouldContinue = new AtomicBoolean(true);
    final Semaphore txnsOutstanding = new Semaphore(100);

    public AdHocMayhemThread(Client client) {
        this.client = client;
    }

    private String nextAdHoc() {
        // 1/5 of all adhocs are MP
        boolean replicated = (counter++ % 5) == 0;

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

    void shutdown() {
        m_shouldContinue.set(false);
    }

    class AdHocCallback implements ProcedureCallback {
        @Override
        public void clientCallback(ClientResponse clientResponse) throws Exception {
            txnsOutstanding.release();
            if (clientResponse.getStatus() != ClientResponse.SUCCESS) {
                System.err.println("Non success in ProcCallback for AdHocMayhemThread");
                System.err.println(((ClientResponseImpl)clientResponse).toJSONString());
            }
        }
    }

    @Override
    public void run() {
        try {
            client.callProcedure("SetupAdHocTables");
        } catch (Exception e) {
            System.err.println("SetupAdHocTables failed in AdHocMayhemThread");
            e.printStackTrace();
            System.exit(-1);
        }


        while (m_shouldContinue.get()) {
            try {
                txnsOutstanding.acquire();
            } catch (InterruptedException e) {
                System.err.println("AdHocMayhemThread interrupted while waiting for permit");
                e.printStackTrace();
                return;
            }

            String sql = nextAdHoc();
            try {
                client.callProcedure(new AdHocCallback(), "@AdHoc", sql);
            }
            catch (Exception e) {
                System.err.println("AdHocMayhemThread failed to run an AdHoc statement");
                e.printStackTrace();
                System.exit(-1);
            }
        }
    }
}
