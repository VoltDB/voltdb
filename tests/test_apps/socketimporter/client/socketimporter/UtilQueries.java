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

package socketimporter.client.socketimporter;

import org.voltcore.logging.VoltLogger;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;

/****
 * sample importer stats output
 *
 * 24> exec @Statistics importer 0;
 * TIMESTAMP      HOST_ID  HOSTNAME                  SITE_ID  IMPORTER_NAME   PROCEDURE_NAME  SUCCESSES  FAILURES  OUTSTANDING_REQUESTS  RETRIES
 * -------------- -------- ------------------------- -------- --------------- --------------- ---------- --------- --------------------- --------
 * 1447181756238        0 my-MacBook-Pro.local         0 SocketImporter  InsertOnly         3959955         0                     0        0
 ****/


public class UtilQueries {
    static VoltLogger log = new VoltLogger("SocketImporter.UtilQueries");

    protected static String getImportStats(Client client) {
        VoltTable importStats = statsCall(client);
        String statsString = null;

        while (importStats.advanceRow()) {
            statsString = importStats.getString("IMPORTER_NAME") + ", " +
                    importStats.getString("PROCEDURE_NAME") + ", " + importStats.getLong("SUCCESSES") + ", " +
                    importStats.getLong("FAILURES") + ", " + importStats.getLong("OUTSTANDING_REQUESTS") + ", " +
                    importStats.getLong("RETRIES");
            //log.info("statsString:" + statsString);
        }
        return statsString;
    }

    protected static long getImportOutstandingRequests(Client client) {
        VoltTable importStats = statsCall(client);
        long outstandingRequests = 0;

        while (importStats.advanceRow()) {
            outstandingRequests = importStats.getLong("OUTSTANDING_REQUESTS");
        }
        return outstandingRequests;
    }

    protected static VoltTable statsCall(Client client) {
        VoltTable importStats = null;

        try {
            importStats = client.callProcedure("@Statistics", "importer", 0).getResults()[0];
        } catch (Exception e) {
            log.error("Stats query failed");
        }
        return importStats;
    }
}

