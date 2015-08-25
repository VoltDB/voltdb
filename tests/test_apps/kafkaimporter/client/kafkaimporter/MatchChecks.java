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

package kafkaimporter.client.kafkaimporter;

import java.io.IOException;

import org.voltcore.logging.VoltLogger;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;


public class MatchChecks {
    static VoltLogger log = new VoltLogger("Benchmark.matchChecks");

    protected static long getMirrorTableRowCount(boolean alltypes, Client client) {
        // check row count in mirror table -- the "master" of what should come back
        // eventually via import
        long mirrorRowCount = 0;
        String countsp = alltypes ? "CountMirror2" : "CountMirror1";


        try {
            VoltTable[] countQueryResult = client.callProcedure(countsp).getResults();
            mirrorRowCount = countQueryResult[0].asScalarLong();
        } catch (Exception e) {
            log.error("Exception from callProcedure " + countsp, e);
            System.exit(-1);
        }
        return mirrorRowCount;
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

    protected static long getExportRowCount(Client client) {
        // get the count of rows exported
        ClientResponse response = doAdHoc(client, "select sum(TOTAL_ROWS_EXPORTED) from exportcounts order by 1;");
        VoltTable[] countQueryResult = response.getResults();
        VoltTable data = countQueryResult[0];
        return data.asScalarLong();
    }

    protected static long getImportRowCount(Client client) {
        // get the count of rows imported
        ClientResponse response = doAdHoc(client, "select sum(TOTAL_ROWS_DELETED) from importcounts order by 1;");
        VoltTable[] countQueryResult = response.getResults();
        VoltTable data = countQueryResult[0];
        return data.asScalarLong();
    }

    public static long getImportTableRowCount(boolean alltypes, Client client) {
        // check row count in import table
        long importRowCount = 0;
        String countsp = alltypes ? "CountImport2" : "CountImport1";

        try {
            VoltTable[] countQueryResult = client.callProcedure(countsp).getResults();
            importRowCount = countQueryResult[0].asScalarLong();
        } catch (Exception e) {
            log.error("Exception from callProcedure " + countsp, e);
            System.exit(-1);
        }
        return importRowCount;
    }
}

