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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Random;

import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;

public class DdlThread extends BenchmarkThread {

    public static AtomicLong progressInd = new AtomicLong(0);
    final Client client;
    final AtomicBoolean m_shouldContinue = new AtomicBoolean(true);
    final AtomicBoolean m_needsBlock = new AtomicBoolean(false);
    final String[] createOrDrop = { "create table anothertable (a int);",
                                    "drop table anothertable if exists;" };
    final Random rnd = new Random();

    public DdlThread(Client client) {
        setName("DdlThread");
        this.client = client;
    }

    void shutdown() {
        m_shouldContinue.set(false);
        this.interrupt();
    }

    @Override
    public void run() {
        int count = 0;
        int errcnt = 0;
        while (m_shouldContinue.get()) {
            // call a ddl transaction
            log.info (createOrDrop[count]);
            try {
                ClientResponse cr = TxnId2Utils.doAdHoc(client, createOrDrop[count]);
                if (cr.getStatus() != ClientResponse.SUCCESS) {
                    hardStop("DDL failed: " + cr.getStatusString());
                } else {
                    log.info("DDL success #" + Long.toString(progressInd.get()) + " : " + createOrDrop[count]);
                    progressInd.getAndIncrement();
                    Benchmark.txnCount.incrementAndGet();
                    errcnt = 0;
                }
            }
            catch (ProcCallException e) {
                ClientResponse cr = e.getClientResponse();
                String ss = cr.getStatusString();
                // nb if rejoining and table exists create table will get table exists but drop table will get rejoin in progress
                if (ss.matches("(?s).*Can't do a catalog update while an elastic join or rejoin is active.*"))
                    errcnt = 0;
                if (errcnt > 1) {
                    hardStop("too many ddl errors", e);
                } else {
                    errcnt++;
                }
            }
            catch (Exception e) {
                hardStop("DdlThread threw an error:", e);
            }
            count = ++count & 1;
            int nextInMs = rnd.nextInt(60) * 1000 + 30000;
            try { Thread.sleep(nextInMs); }
            catch (Exception e) {}
        }
        log.info(getName() + " thread has stopped");
    }
}
