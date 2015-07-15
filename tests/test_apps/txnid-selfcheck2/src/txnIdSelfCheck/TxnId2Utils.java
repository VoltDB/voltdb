package txnIdSelfCheck;

import java.io.IOException;

import org.voltcore.logging.VoltLogger;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;

public enum TxnId2Utils {;

    static VoltLogger log = new VoltLogger("TxnId2Utils");

    static ClientResponse doAdHoc(Client client, String query) throws NoConnectionsException, IOException, ProcCallException {
        Boolean sleep = false;
        Boolean noConnections = false;
        Boolean timedOutOnce = false;
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
                if (!timedOutOnce && ss.matches("(?s).*No response received in the allotted time.*"))
                    /* allow a generic timeout but only once so that we don't risk masking error conditions */
                    {timedOutOnce = true;}
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
