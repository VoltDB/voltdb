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

import java.io.IOException;
import java.security.SecureRandom;

import org.voltcore.logging.VoltLogger;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;

public enum TxnId2Utils {;

    static VoltLogger log = new VoltLogger("TxnId2Utils");

    static ClientResponse doAdHoc(Client client, String query) throws ProcCallException {
        return doProcCall(client, "@AdHoc", query);
    }

    static boolean isConnectionTransactionOrCatalogIssue(String statusString) {
        return statusString.matches("(?s).*AdHoc transaction -?[0-9]+ wasn.t planned against the current catalog version.*") ||
                statusString.matches(".*Connection to database host \\(.*\\) was lost before a response was received.*") ||
                statusString.matches(".*Transaction dropped due to change in mastership. It is possible the transaction was committed.*") ||
                statusString.matches("(?s).*Transaction being restarted due to fault recovery or shutdown.*") ||
                statusString.matches("(?s).*Invalid catalog update.  Catalog or deployment change was planned against one version of the cluster configuration but that version was no longer live.*") ||
                statusString.matches("(?s).*Ad Hoc Planner task queue is full.*") ||
                statusString.matches("(?s).*Transaction Interrupted.*");
    }

    static boolean isServerUnavailableIssue(String statusString) {
        return statusString.matches("(?s).*No response received in the allotted time.*") ||
                statusString.matches(".*Server is currently unavailable; try again later.*") ||
                statusString.matches(".*Server is paused.*") ||
                statusString.matches("(?s).*Server is shutting down.*") ||
                statusString.matches(".*Procedure call not queued: timed out waiting for host connection.*") ||
                statusString.matches("(?s).*VoltDB failed to create the transaction internally.*it should be safe to resend the work, as the work was never started.*"); /*-5 SERVER_UNAVAILABLE*/
    }

    static boolean isConnectionTransactionCatalogOrServerUnavailableIssue(String statusString) {
        return isConnectionTransactionOrCatalogIssue(statusString) ||
                isServerUnavailableIssue(statusString);
    }

    static boolean isTransactionStateIndeterminate(ClientResponse cr) {
        String statusString = cr.getStatusString();
        return (statusString.matches("(?s).*No response received in the allotted time.*") ||
                statusString.matches(".*Connection to database host \\(.*\\) was lost before a response was received.*"));
    }

    static boolean isServerUnavailableStatus(byte status) {
        switch (status) {
        case ClientResponse.CONNECTION_LOST:
        case ClientResponse.CONNECTION_TIMEOUT:
        case ClientResponse.SERVER_UNAVAILABLE:
        case ClientResponse.CLIENT_REQUEST_TIMEOUT:
        case ClientResponse.CLIENT_RESPONSE_TIMEOUT:
        case ClientResponse.RESPONSE_UNKNOWN:
            return true;
        default:
            return false;
        }
    }


    static ClientResponse doProcCall(Client client, String proc, Object... parms) throws ProcCallException {
        return doProcCall(client, proc, false, parms);
    }
    static ClientResponse doProcCallOneShot(Client client, String proc, Object... parms) throws ProcCallException {
        return doProcCall(client, proc, true, parms);
    }
    static ClientResponse doProcCall(Client client, String proc, boolean oneshot, Object... parms) throws ProcCallException {
        Boolean sleep = false;
        Boolean noConnections = false;
        ClientResponse cr = null;
        while (true) {
            try {
                if (proc == "@AdHoc")
                    cr = client.callProcedure("@AdHoc", (String) parms[0]);
                else
                    cr = client.callProcedure(proc, parms);
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
                cr = e.getClientResponse();
                String ss = cr.getStatusString();
                log.debug(ss);
                if (isConnectionTransactionOrCatalogIssue(ss)
                    ) { /* continue */ }
                else if (isServerUnavailableIssue(ss)) {
                    sleep = true;
                }
                else {
                    throw e;
                }
            }
            if (oneshot)
                return cr;
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

    static final String AB = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    static SecureRandom rnd = new SecureRandom();

    static String randomString( int len ){
        StringBuilder sb = new StringBuilder( len );
        for( int i = 0; i < len; i++ )
            sb.append( AB.charAt( rnd.nextInt(AB.length()) ) );
        return sb.toString();
    }
}
