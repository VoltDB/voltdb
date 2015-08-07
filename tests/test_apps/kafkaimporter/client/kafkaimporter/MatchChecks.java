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
import java.lang.InterruptedException;

import org.voltcore.logging.VoltLogger;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.client.NoConnectionsException;


public class MatchChecks {
    static VoltLogger log = new VoltLogger("Benchmark.matchChecks");
    final static String DELETE_ROWS = "DeleteRows";

    static class DeleteCallback implements ProcedureCallback {
        final String proc;
        final long key;

        DeleteCallback(String proc, long key) {
            this.proc = proc;
            this.key = key;
        }

        @Override
        public void clientCallback(ClientResponse clientResponse) {

            // Make sure the procedure succeeded. If not,
            // report the error.
            if (clientResponse.getStatus() != ClientResponse.SUCCESS) {
                String msg = String.format("%s k: %12d, callback fault: %s", proc, key, clientResponse.getStatusString());
                log.error(msg);
              }
         }
    }

    protected static long getMirrorTableRowCount(Client client) {
        // check row count in mirror table -- the "master" of what should come back
        // eventually via import
        long mirrorRowCount = 0;

        try {
            VoltTable[] countQueryResult = client.callProcedure("CountMirror").getResults();
            mirrorRowCount = countQueryResult[0].asScalarLong();
        } catch (Exception e) {
            log.error("Exception from callProcedure", e);
            System.exit(-1);
        }
        return mirrorRowCount;
    }

    protected static long getExportRowCount(Client client) {
        // get the count of rows imported
        long exportRowCount = 0;

        while (true) {
            try {
                ClientResponse response = client.callProcedure("@AdHoc", "select sum(TOTAL_ROWS_EXPORTED) from exportcounts order by 1;");
                if (response.getStatus() != ClientResponse.SUCCESS) {
                    log.warn("command failed: " + response.getStatusString());
                    continue;
                }
                VoltTable[] countQueryResult = response.getResults();
                VoltTable data = countQueryResult[0];
                long nrows = data.getRowCount();
                if (nrows > 0)
                    exportRowCount = data.asScalarLong();
                break;
            } catch (NoConnectionsException e) {
                try { Thread.sleep(3); } catch (InterruptedException ex) { }
            } catch (Exception e) {
                log.error("Exception from callProcedure", e);
                System.exit(-1);
            }
        }
        return exportRowCount;
    }

    protected static long getImportRowCount(Client client) {
        // get the count of rows imported
        long importRowCount = 0;

        try {
            VoltTable[] countQueryResult = client.callProcedure("@AdHoc", "select sum(TOTAL_ROWS_DELETED) from importcounts order by 1;").getResults();
            VoltTable data = countQueryResult[0];
            long nrows = data.getRowCount();
            if (nrows > 0)
                importRowCount = data.asScalarLong();
        } catch (Exception e) {
            log.error("Exception from callProcedure", e);
        }
        return importRowCount;
    }

    protected static long findAndDeleteMatchingRows(Client client) {
        long rows = 0;
        VoltTable results = null;

        try {
            results = client.callProcedure("MatchRows").getResults()[0];
        } catch (Exception e) {
            log.error("Exception from callProcedure", e);
            System.exit(-1);
        }

        log.info("getRowCount(): " + results.getRowCount());
        while (results.advanceRow()) {
            long key = results.getLong(0);
            try {
                client.callProcedure(new DeleteCallback(DELETE_ROWS, key), DELETE_ROWS, key);
            } catch (Exception e) {
                log.error("Exception from callProcedure", e);
                System.exit(-1);
            }
            rows++;
        }
        return rows;
    }

    public static long getImportTableRowCount(Client client) {
        // check row count in import table
        long importRowCount = 0;
        try {
            VoltTable[] countQueryResult = client.callProcedure("CountImport").getResults();
            importRowCount = countQueryResult[0].asScalarLong();
        } catch (Exception e) {
            log.error("Exception from callProcedure", e);
            System.exit(-1);
        }
        return importRowCount;
    }
}

