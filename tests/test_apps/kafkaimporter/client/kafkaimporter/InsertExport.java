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
import java.util.concurrent.atomic.AtomicLong;

import org.voltcore.logging.VoltLogger;
import org.voltcore.logging.Level;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcedureCallback;

public class InsertExport {
    static VoltLogger log = new VoltLogger("Benchmark.insertExport");
    final Client m_client;
    static AtomicLong m_rowsAdded;
    final static String INSERT_PN = "InsertFinal";
    static String m_export_sp;

    public InsertExport(boolean alltypes, Client client, AtomicLong rowsAdded) {
        m_client = client;
        m_rowsAdded = rowsAdded;
        m_export_sp = alltypes ? "InsertExport2" : "InsertExport";
        log.info("Insert Export SP is: " + m_export_sp);
    }

    public void insertExport(long key, long value) {
        try {
            m_client.callProcedure(new InsertCallback(m_export_sp, key, value), m_export_sp, key, value);
        } catch (NoConnectionsException e) {
            log.rateLimitedLog(10, Level.WARN, e, "%s", m_export_sp);
            // log.warn("NoConnectionsException calling stored procedure" + m_export_sp);
            try {
                Thread.sleep(3);
            } catch (InterruptedException ex) { }
        } catch (Exception e) {
            log.warn("Exception calling stored procedure" + m_export_sp, e);
        }
    }

    public void insertFinal(long key, long value) {
        try {
            m_client.callProcedure(new InsertCallback(INSERT_PN, key, value), INSERT_PN, key, value);
        } catch (Exception e) {
            log.info("Exception calling stored procedure InsertFinal", e);
            System.exit(-1);
        }
    }

    static class InsertCallback implements ProcedureCallback {
        final String proc;
        final long key;
        final long value;

        InsertCallback(String proc, long key, long value) {
            this.proc = proc;
            this.key = key;
            this.value = value;
        }

        @Override
        public void clientCallback(ClientResponse clientResponse)
                throws Exception {
            if (clientResponse.getStatus() != ClientResponse.SUCCESS) {
                if (!clientResponse.getStatusString().contains("Server is paused and is available in read-only mode") &&
                        !clientResponse.getStatusString().contains("was lost before a response was received")) {
                    log.rateLimitedLog(10, Level.WARN, null, "%s k: %12d, v: %12d callback fault: %s",
                            proc, key, value, clientResponse.getStatusString());
                }
            } else {
                m_rowsAdded.incrementAndGet();
            }
        }

    }
}
