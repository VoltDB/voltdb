/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

import org.voltcore.logging.VoltLogger;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;


public class MatchChecks {
    static VoltLogger log = new VoltLogger("Benchmark.matchChecks");

    protected static long getMirrorTableRowCount(boolean alltypes, Client client) {
        // check row count in mirror table -- the "master" of what should come back
        // eventually via import
        String table = alltypes ? "KafkaMirrorTable2" : "KafkaMirrorTable1";
        ClientResponse response = doAdHoc(client, "select count(*) from " + table);
        VoltTable[] countQueryResult = response.getResults();
        VoltTable data = countQueryResult[0];
        if (data.asScalarLong() == VoltType.NULL_BIGINT)
            return 0;
        return data.asScalarLong();
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
        if (data.asScalarLong() == VoltType.NULL_BIGINT)
            return 0;
        return data.asScalarLong();
    }

    protected static long getImportRowCount(Client client) {
        // get the count of rows imported
        ClientResponse response = doAdHoc(client, "select sum(TOTAL_ROWS_DELETED) from importcounts order by 1;");
        VoltTable[] countQueryResult = response.getResults();
        VoltTable data = countQueryResult[0];
        if (data.asScalarLong() == VoltType.NULL_BIGINT)
            return 0;
        return data.asScalarLong();
    }

    protected static long checkRowMismatch(Client client) {
        // check if any rows failed column by colunn comparison so we can fail fast
        ClientResponse response = doAdHoc(client, "select key from importcounts where value_mismatch = 1 limit 1;");
        VoltTable[] result = response.getResults();
        if (result[0].getRowCount() == 0) // || result[0].asScalarLong() == 0)
            return 0;
        return result[0].asScalarLong();
    }

    public static long getImportTableRowCount(boolean alltypes, Client client) {
        // check row count in import table
        String table = alltypes ? "KafkaImportTable2" : "KafkaImportTable1";
        ClientResponse response = doAdHoc(client, "select count(*) from " + table);
        VoltTable[] countQueryResult = response.getResults();
        VoltTable data = countQueryResult[0];
        if (data.asScalarLong() == VoltType.NULL_BIGINT)
            return 0;
        return data.asScalarLong();
    }

    public static boolean checkPounderResults(long expected_rows, Client client) {
        // make sure import table has expected number of rows, and without gaps
        // we check the row count, then use min & max to infer the range is complete
        long importRowCount = 0;
        long importMax = 0;
        long importMin = 0;

        // check row count in import table
        // String table = alltypes ? "KafkaImportTable2" : "KafkaImportTable1";

        ClientResponse response = doAdHoc(client, "select count(key), min(key), max(key) from kafkaimporttable1");
        VoltTable countQueryResult = response.getResults()[0];
        countQueryResult.advanceRow();
        importRowCount = (long) countQueryResult.get(0, VoltType.BIGINT);
        importMin = (long) countQueryResult.get(1, VoltType.BIGINT);
        importMax = (long) countQueryResult.get(2, VoltType.BIGINT);

        if (importRowCount != expected_rows) {
            log.error(expected_rows + " expected. " + importRowCount + " received.");
            return false;
        }

        if (importMax == VoltType.NULL_BIGINT) {
            importMax = 0;
        }
        if (importMin == VoltType.NULL_BIGINT) {
            importMin = 0;
        }
        if ((importMax-importMin+1) != expected_rows) {
            log.error(expected_rows + " expected. " + (importMax-importMin+1) + " rows received.");
            return false;
        }
        return true;
    }

    protected static String getImportStats(Client client) {
        VoltTable importStats = statsCall(client);
        String statsString = null;

        while (importStats.advanceRow()) {
            statsString = importStats.getString("IMPORTER_NAME") + ", " +
                    importStats.getString("PROCEDURE_NAME") + ", " + importStats.getLong("SUCCESSES") + ", " +
                    importStats.getLong("FAILURES") + ", " + importStats.getLong("OUTSTANDING_REQUESTS") + ", " +
                    importStats.getLong("RETRIES");
            log.info("statsString:" + statsString);
        }
        return statsString;
    }

    protected static String getClusterState(Client client) {
        VoltTable sysinfo = null;

        try {
            sysinfo = client.callProcedure("@SystemInformation", "OVERVIEW").getResults()[0];
        } catch (Exception e) {
            log.warn("system info query failed");
            return "";
        }

        for (int i = 0; i < sysinfo.getRowCount(); i++)
        {
            sysinfo.advanceRow();
            if (sysinfo.get("KEY", VoltType.STRING).equals("CLUSTERSTATE"))
            {
                return (String) sysinfo.get("VALUE",VoltType.STRING);
            }
        }
        return "";
    }

    protected static long[] getImportValues(Client client) {
        VoltTable importStats = statsCall(client);
        long stats[] = {0, 0, 0, 0};

        while (importStats.advanceRow()) {
            int statnum = 0;
            log.info("getImportValues: " + importStats.getString("PROCEDURE_NAME"));
            stats[statnum] = importStats.getLong("SUCCESSES"); log.info("\tSUCCESSES: " + stats[statnum++]);
            stats[statnum] = importStats.getLong("FAILURES"); log.info("\tFAILURES: " + stats[statnum++]);
            stats[statnum] = importStats.getLong("OUTSTANDING_REQUESTS"); log.info("\tOUTSTANDING_REQUESTS: " + stats[statnum++]);
            stats[statnum] = importStats.getLong("RETRIES"); log.info("\tRETRIES: " + stats[statnum++]);
        }

        return stats;
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

