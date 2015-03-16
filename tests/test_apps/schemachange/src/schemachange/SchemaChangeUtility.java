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

package schemachange;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

import org.voltcore.logging.VoltLogger;
import org.voltdb.ClientResponseImpl;
import org.voltdb.TableHelper;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;

public class SchemaChangeUtility
{
    static VoltLogger log = new VoltLogger("HOST");

    /**
     * Call a procedure and check the return code.
     * Success just returns the result to the caller.
     * Unpossible errors end the process.
     * Some errors will retry the call until the global progress timeout with various waits.
     * After the global progress timeout, the process is killed.
     */
    static ClientResponse callROProcedureWithRetry(Client client, String procName, int timeout, Object... params) {
        long startTime = System.currentTimeMillis();
        long now = startTime;

        while (now - startTime < (timeout * 1000)) {
            ClientResponse cr = null;

            try {
                cr = client.callProcedure(procName, params);
            }
            catch (ProcCallException e) {
                log.debug("callROProcedureWithRetry operation exception:", e);
                cr = e.getClientResponse();
            }
            catch (NoConnectionsException e) {
                log.debug("callROProcedureWithRetry operation exception:", e);
                // wait a bit to retry
                try { Thread.sleep(1000); } catch (InterruptedException e1) {}
            }
            catch (IOException e) {
                log.debug("callROProcedureWithRetry operation exception:", e);
                // IOException is not cool man
                logStackTrace(e);
                System.exit(-1);
            }

            if (cr != null) {
                if (cr.getStatus() != ClientResponse.SUCCESS) {
                    log.debug("callROProcedureWithRetry operation failed: " + ((ClientResponseImpl)cr).toJSONString());
                }
                switch (cr.getStatus()) {
                case ClientResponse.SUCCESS:
                    // hooray!
                    return cr;
                case ClientResponse.CONNECTION_LOST:
                case ClientResponse.CONNECTION_TIMEOUT:
                    // can retry after a delay
                    try { Thread.sleep(5 * 1000); } catch (Exception e) {}
                    break;
                case ClientResponse.RESPONSE_UNKNOWN:
                    // can try again immediately - cluster is up but a node died
                    break;
                case ClientResponse.SERVER_UNAVAILABLE:
                    // shouldn't be in admin mode (paused) in this app, but can retry after a delay
                    try { Thread.sleep(30 * 1000); } catch (Exception e) {}
                    break;
                case ClientResponse.GRACEFUL_FAILURE:
                    //log.error(_F("GRACEFUL_FAILURE response in procedure call for: %s", procName));
                    //log.error(((ClientResponseImpl)cr).toJSONString());
                    //logStackTrace(new Throwable());
                    return cr; // caller should always check return status
                case ClientResponse.UNEXPECTED_FAILURE:
                case ClientResponse.USER_ABORT:
                    log.error(String.format("Error in procedure call for: %s", procName));
                    log.error(((ClientResponseImpl)cr).toJSONString());
                    // for starters, I'm assuming these errors can't happen for reads in a sound system
                    assert(false);
                    System.exit(-1);
                }
            }

            now = System.currentTimeMillis();
        }

        log.error(String.format("Error no progress timeout (%d seconds) reached, terminating", timeout));
        System.exit(-1);
        return null;
    }

    static void logStackTrace(Throwable t) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw, true);
        t.printStackTrace(pw);
        log.error(sw.toString());
    }

    /**
     * Find the largest pkey value in the table.
     */
    static long maxId(Client client, VoltTable t, int timeout) {
        if (t == null) {
            return 0;
        }
        ClientResponse cr = callROProcedureWithRetry(client, "@AdHoc", timeout,
                String.format("select pkey from %s order by pkey desc limit 1;", TableHelper.getTableName(t)));
        assert(cr.getStatus() == ClientResponse.SUCCESS);
        VoltTable result = cr.getResults()[0];
        return result.getRowCount() > 0 ? result.asScalarLong() : 0;
    }

    /**
     * Die happily or tragically with a formatted message.
     * Simply exits if message is null.
     */
    static void die(boolean happy, String format, Object... params) {
        if (format != null) {
            String message = String.format(format, params);
            if (happy) {
                log.info(message);
            }
            else {
                log.error(message);
            }
        }
        System.exit(happy ? 0 : -1);
    }
}
