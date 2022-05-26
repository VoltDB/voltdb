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

/*
 * DDL in the Java client?
 *
 * It's because there can't be two different socket import streams
 * in the deployment file.
 *
 * So the strategy is the create the target table, importtable partitioned,
 * run the test, then it's possible to drop the table and its SP's and recreate
 * it without partitioning.
 *
 * If at some point this limitation is eliminated -- ENG-9074, then the
 * DDL in the client could be eliminated.
 *
 * However it might be a useful test variant since there's not much DDL
 * in the system test client.
 */
package socketimporter.client.socketimporter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

import org.voltcore.utils.Pair;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;
import org.voltcore.logging.VoltLogger;

public class DataUtils {
    static VoltLogger log = new VoltLogger("DataUtils");
    static Queue<Pair<String, String>> m_queue;
    static Queue<Pair<String, String>> m_delete_queue;
    Client m_client;
    private static final int KEY = 0;
    private static final int VALUE = 1;

    private static final String m_select = "SelectOnly"; // would use default IMPORTTABLE.select but it's not created on replicated table
    private static final String m_delete = "IMPORTTABLE.delete";
    private static final String m_max = "SelectMaxTime";

    public DataUtils(Queue<Pair<String, String>> q, Queue<Pair<String, String>> dq, Client c, boolean partitioned) {
        m_client = c;
        m_queue = q;
        m_delete_queue = dq;
    }

    public void processQueue() {
        while (m_queue.size() > 0 || m_delete_queue.size() > 0) {
            Pair<String, String> p = m_queue.poll();

            try {
                if (p != null) {
                    String key = p.getFirst();
                    boolean ret = m_client.callProcedure(new SelectCallback(m_queue, p, key), m_select, key);
                    if (!ret) {
                        log.info("Select call failed!");
                    }
                }
                Pair<String, String> p2 = m_delete_queue.poll();
                if (p2 != null) {
                    boolean ret = m_client.callProcedure(new DeleteCallback(m_delete_queue, p2), m_delete, p2.getFirst());
                    if (!ret) {
                        log.info("Delete call failed!");
                    }
                }
                AsyncBenchmark.rowsChecked.incrementAndGet();
            } catch (NoConnectionsException e) {
                e.printStackTrace();
                System.exit(1);
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(1);
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(1);
            }
        }
    }

    public long maxInsertTime() {
            ClientResponse response = null;
            try {
                response = m_client.callProcedure(m_max);
            } catch (NoConnectionsException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ProcCallException e) {
                e.printStackTrace();
            }
            VoltTable[] countQueryResult = response.getResults();
            VoltTable data = countQueryResult[0];
            if (data.asScalarLong() == VoltType.NULL_BIGINT)
                return 0;
            return data.asScalarLong();
    }

    public void ddlSetup(boolean partitioned) {
        final String[] DDLStmts = {
                "CREATE TABLE IMPORTTABLE ( " +
                "  key      varchar(250) not null " +
                ", value    varchar(1048576 BYTES) not null " +
                ", insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL " +
                ", PRIMARY KEY (key))",
                "CREATE PROCEDURE SelectOnly as select * from importtable where key = ?",
                "CREATE PROCEDURE InsertOnly as insert into IMPORTTABLE(key, value) VALUES(?, ?)",
                "CREATE PROCEDURE SelectMaxTime as select since_epoch(millis, max(insert_time)) from IMPORTTABLE",
        };
        final String[] PartitionStmts = {
                "PARTITION table IMPORTTABLE ON COLUMN key",
                "PARTITION PROCEDURE InsertOnly ON TABLE importtable COLUMN key",
                "PARTITION PROCEDURE SelectOnly ON TABLE importtable COLUMN key"
        };
        dropTables();
        try {
            for (int i = 0; i < DDLStmts.length; i++) {
                m_client.callProcedure("@AdHoc",
                        DDLStmts[i]).getResults();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (partitioned) {
            try {
                for (int i = 0; i < PartitionStmts.length; i++) {
                    m_client.callProcedure("@AdHoc",
                            PartitionStmts[i]).getResults();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void dropTables() {
        final String[] dropStmts = {
            "procedure InsertOnly IF EXISTS",
            "procedure SelectMaxTime IF EXISTS",
            "procedure SelectOnly IF EXISTS",
            "table IMPORTTABLE IF EXISTS"
        };
        try {
            for (int i = 0; i < dropStmts.length; i++) {
                m_client.callProcedure("@AdHoc",
                        "DROP " + dropStmts[i]).getResults();
            }
        } catch (IOException | ProcCallException e) {
            e.printStackTrace();
        }
    }

    static class SelectCallback implements ProcedureCallback {
        private static final int KEY = 0;
        private static final int VALUE = 1;

        Pair<String, String> m_pair;
        Queue<Pair<String, String>> m_queue;
        String m_key;

        public SelectCallback(Queue<Pair<String, String>> q, Pair<String, String> p, String key) {
            m_pair = p;
            m_queue = q;
            m_key = key;
        }

        @Override
        public void clientCallback(ClientResponse response)
                throws Exception {
            if (response.getStatus() != ClientResponse.SUCCESS) {
                log.info(response.getStatusString());
                return;
            }

            List<String> pair = getDataFromResponse(response);
            String key, value;
            if (pair.size() == 2) {
                key = pair.get(KEY);
                value = pair.get(VALUE);
                m_delete_queue.offer(m_pair);
            } else {
                // push the tuple back onto the queue we can try again
                m_queue.offer(m_pair);
                return;
            }

            if (!value.equals(m_pair.getSecond())) {
                log.info("Pair from DB: " + key + ", " + value);
                log.info("Pair from queue: " + m_pair.getFirst() + ", " + m_pair.getSecond());
                AsyncBenchmark.rowsMismatch.incrementAndGet();
            }
        }

        private List<String> getDataFromResponse(ClientResponse response) {
            List<String> m_pair = new ArrayList<String>();
            VoltTable[] m_results = response.getResults();
            if (m_results.length == 0) {
                log.info("zero length results");
                return m_pair;
            }
            VoltTable recordset = m_results[0];
            if (recordset.advanceRow()) {

                m_pair.add((String) recordset.get(KEY, VoltType.STRING));
                m_pair.add((String) recordset.get(VALUE, VoltType.STRING));
            }
            return m_pair;
        }
    }

    static class DeleteCallback implements ProcedureCallback {

        Pair<String, String> m_pair;
        Queue<Pair<String, String>> m_queue;

        public DeleteCallback(Queue<Pair<String, String>> q, Pair<String, String> p) {
            m_pair = p;
            m_queue = q;
        }

        @Override
        public void clientCallback(ClientResponse response)
                throws Exception {
            if (response.getStatus() != ClientResponse.SUCCESS) {
                log.info(response.getStatusString());
                return;
            }
            m_queue.remove(m_pair);

        }
    }
}
