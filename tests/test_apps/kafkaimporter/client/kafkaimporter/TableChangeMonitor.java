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
package client.kafkaimporter;

import java.io.IOException;

import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ProcCallException;
import org.voltcore.logging.VoltLogger;

public class TableChangeMonitor {
    static VoltLogger log = new VoltLogger("Benchmark.tableChangeMonitor");

    Client client;
    String table = "";
    String type = "";

    public TableChangeMonitor(Client client, String type, String table) {
        this.type = type;
        this.table = table;
        this.client = client;
    }

    /**
     * Checks the export table to make sure that everything has been successfully
     * processed.
     * @throws ProcCallException
     * @throws IOException
     * @throws InterruptedException
     */
    public boolean waitForStreamedAllocatedMemoryZero() throws ProcCallException,IOException,InterruptedException {
        boolean passed = false;

        VoltTable stats = null;
        long ftime = 0;
        long st = System.currentTimeMillis();
        //Wait 10 mins only
        long end = st + (10 * 60 * 1000);
        while (true) {
            stats = client.callProcedure("@Statistics", "export", 0).getResults()[0];
            boolean passedThisTime = true;
            long ctime = System.currentTimeMillis();
            if (ctime > end) {
                log.info("Waited too long...");
                log.info(stats);
                break;
            }
            if (ctime - st > (3 * 60 * 1000)) {
                log.info(stats);
                st = System.currentTimeMillis();
            }
            long ts = 0;
            while (stats.advanceRow()) {
                String ttable = stats.getString("TABLE_NAME");
                Long tts = stats.getLong("TIMESTAMP");
                //Get highest timestamp and watch it change
                if (tts > ts) {
                    ts = tts;
                }
                if (stats.getLong("TUPLE_PENDING") != 0 && stats.getString("ACTIVE").equalsIgnoreCase("TRUE") ) {
                    passedThisTime = false;
                    log.info(ttable + ": Partition Not Zero.");
                    break;
                }

            }
            if (passedThisTime) {
                if (ftime == 0) {
                    ftime = ts;
                    continue;
                }
                //we got 0 stats 2 times in row with diff highest timestamp.
                if (ftime != ts) {
                    passed = true;
                    break;
                }
                log.info(table + " quiescing but not ready to declare victory.");
            }
            Thread.sleep(5000);
        }
        log.info(table + " status is: " + passed);
        log.info(stats);
        return passed;
    }

}
