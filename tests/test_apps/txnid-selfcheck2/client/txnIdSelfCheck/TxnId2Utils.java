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

import java.io.IOException;

import org.voltcore.logging.VoltLogger;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;

public enum TxnId2Utils {;

    static VoltLogger log = new VoltLogger("TxnId2Utils");

    static ClientResponse doAdHoc(Client client, String query) throws NoConnectionsException, IOException, ProcCallException {
        Boolean sleep = false;
        Boolean noConnections = false;
        while (true) {
            try {
                ClientResponse cr = client.callProcedure("@AdHoc", query);
                if (cr.getStatus() == ClientResponse.SUCCESS) {
                    Benchmark.txnCount.incrementAndGet();
                    return cr;
                } else {
                    log.debug(cr.getStatusString());
                    Benchmark.hardStop("unexpected response", cr);
                }
            } catch (NoConnectionsException e) {
                noConnections = true;
            } catch (IOException e) {
                Benchmark.hardStop(e);
            } catch (ProcCallException e) {
                ClientResponse cr = e.getClientResponse();
                String ss = cr.getStatusString();
                log.debug(ss);
                if (/*cr.getStatus() == ClientResponse.USER_ABORT &&*/
                    (ss.matches("(?s).*AdHoc transaction [0-9]+ wasn.t planned against the current catalog version.*") ||
                     ss.matches(".*Connection to database host \\(.*\\) was lost before a response was received.*") ||
                     ss.matches(".*Transaction dropped due to change in mastership. It is possible the transaction was committed.*") ||
                     ss.matches("(?s).*Transaction being restarted due to fault recovery or shutdown.*"))) {
                    }
                else if (ss.matches(".*Server is currently unavailable; try again later.*")) {
                    sleep = true;
                }
                else {
                    throw e;
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

    static long getRowCount(Client client, String tableName) throws NoConnectionsException, IOException, ProcCallException {
        ClientResponse cr = doAdHoc(client, "select count(*) from " + tableName + ";");
        return cr.getResults()[0].asScalarLong();
    }
}
