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

import java.io.IOException;

import org.voltcore.logging.VoltLogger;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;

/****
 * sample importer stats output
 *
 * 24> exec @Statistics importer 0;
 * TIMESTAMP      HOST_ID  HOSTNAME                  SITE_ID  IMPORTER_NAME   PROCEDURE_NAME  SUCCESSES  FAILURES  OUTSTANDING_REQUESTS  RETRIES
 * -------------- -------- ------------------------- -------- --------------- --------------- ---------- --------- --------------------- --------
 * 1447181756238        0 peters-MacBook-Pro.local         0 SocketImporter  InsertOnly         3959955         0                     0        0
 ****/


public class UtilQueries {
    static VoltLogger log = new VoltLogger("SocketImporter.UtilQueries");

    protected static long getImportStats(Client client) {
        // check row count in mirror table -- the "master" of what should come back
        // eventually via import
        VoltTable importStats = null;
        long outstandingRequests = 0;
        try {
            importStats = client.callProcedure("@Statistics", "importer", 0).getResults()[0];
        } catch (Exception e) {
            System.err.print("Stats query failed");
        }
        while (importStats.advanceRow()) {
            System.out.println("Importer: " + importStats.getString(4));
            System.out.println("Procedure: " + importStats.getString(5));
            System.out.println("Successes: " + importStats.getLong(6));
            System.out.println("Failures: " + importStats.getLong(7));
            System.out.println("Outstanding Requests: " + importStats.getLong(8));
            System.out.println("Retries: " + importStats.getLong(9));
        }
        return outstandingRequests;
    }

    static ClientResponse doAdHoc(Client client, String query) {
        /* a very similar method is used in txnid2::txnidutils, try to keep them in sync */
        Boolean sleep = false;
        Boolean noConnections = false;
        Boolean timedOutOnce = false;
        while (true) {
            try {
                ClientResponse cr = client.callProcedure("@AdHoc", query);
                if (cr.getStatus() == ClientResponse.SUCCESS) {
                    return cr;
                } else {
                    log.debug(cr.getStatusString());
                    log.error("unexpected response");
                    System.exit(-1);
                }
            } catch (NoConnectionsException e) {
                noConnections = true;
            } catch (IOException e) {
                log.error("IOException",e);
                System.exit(-1);
            } catch (ProcCallException e) {
                ClientResponse cr = e.getClientResponse();
                String ss = cr.getStatusString();
                log.debug(ss);
                if (!timedOutOnce && ss.matches("(?s).*No response received in the allotted time.*"))
                    /* allow a generic timeout but only once so that we don't risk masking error conditions */
                {   timedOutOnce = false; //false;
                    sleep = true;
                }
                else if (/*cr.getStatus() == ClientResponse.USER_ABORT &&*/
                        (ss.matches("(?s).*AdHoc transaction [0-9]+ wasn.t planned against the current catalog version.*") ||
                                ss.matches(".*Connection to database host \\(.*\\) was lost before a response was received.*") ||
                                ss.matches(".*Transaction dropped due to change in mastership. It is possible the transaction was committed.*") ||
                                ss.matches("(?s).*Transaction being restarted due to fault recovery or shutdown.*") ||
                                ss.matches("(?s).*Invalid catalog update.  Catalog or deployment change was planned against one version of the cluster configuration but that version was no longer live.*")
                        )) {}
                else if (ss.matches(".*Server is currently unavailable; try again later.*") ||
                        ss.matches(".*Server is paused and is currently unavailable.*")) {
                    sleep = true;
                }
                else {
                    log.error("Unexpected ProcCallException", e);
                    System.exit(-1);
                }
            }
            if (sleep | noConnections) {
                try { Thread.sleep(3000); } catch (Exception f) { }
                sleep = false;
                if (noConnections)
                    while (client.getConnectedHostList().size() == 0);
                noConnections = false;
            }
        }
    }
}

