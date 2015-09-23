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
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

import org.voltcore.utils.Pair;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcedureCallback;

public class CheckData {

    static Queue<Pair<Long, Long>> m_queue;
    static Queue<Pair<Long, Long>> m_delete_queue;
    Client m_client;
    private static final int VALUE = 1;
    private static final int KEY = 0;

    private static final String MY_SELECT_PROCEDURE = "IMPORTTABLE2.select";
    private static final String MY_DELETE_PROCEDURE = "IMPORTTABLE2.delete";

    public CheckData(Queue<Pair<Long, Long>> q, Queue<Pair<Long, Long>> dq, Client c) {
        m_client = c;
        m_queue = q;
        m_delete_queue = dq;
    }

    public void processQueue() {
        while (m_queue.size() > 0 || m_delete_queue.size() > 0) {
            Pair<Long, Long> p = m_queue.poll();

            try {
                if (p != null) {
                    Long key = p.getFirst();
                    boolean ret = m_client.callProcedure(new SelectCallback(m_queue, p, key), MY_SELECT_PROCEDURE, key);
                    if (!ret) {
                        System.out.println("Select call failed!");
                    }
                }
                Pair<Long, Long> p2 = m_delete_queue.poll();
                if (p2 != null) {
                    boolean ret = m_client.callProcedure(new DeleteCallback(m_delete_queue, p2), MY_DELETE_PROCEDURE, p2.getFirst());
                    if (!ret) {
                        System.out.println("Delete call failed!");
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

    static class SelectCallback implements ProcedureCallback {

        private static final int KEY = 0;
        private static final int VALUE = 1;

        Pair<Long, Long> m_pair;
        Queue<Pair<Long, Long>> m_queue;
        Long m_key;

        public SelectCallback(Queue<Pair<Long, Long>> q, Pair<Long, Long> p, Long key) {
            m_pair = p;
            m_queue = q;
            m_key = key;
        }

        @Override
        public void clientCallback(ClientResponse response)
                throws Exception {
            if (response.getStatus() != ClientResponse.SUCCESS) {
                System.out.println(response.getStatusString());
                return;
            }

            List<Long> pair = getDataFromResponse(response);
            Long key, value;
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
                System.out.println("Pair from DB: " + key + ", " + value);
                System.out.println("Pair from queue: " + m_pair.getFirst() + ", " + m_pair.getSecond());
                AsyncBenchmark.rowsMismatch.incrementAndGet();
            }
        }

        private List<Long> getDataFromResponse(ClientResponse response) {
            List<Long> m_pair = new ArrayList<Long>();
            //Long[] m_pairString = new Long[0];
            VoltTable[] m_results = response.getResults();
            if (m_results.length == 0) {
                System.out.println("zero length results");
                return m_pair;
            }
            VoltTable recordset = m_results[0];
            if (recordset.advanceRow()) {

                m_pair.add((Long) recordset.get(KEY, VoltType.BIGINT));
                m_pair.add((Long) recordset.get(VALUE, VoltType.BIGINT));
            }
            return m_pair;
        }

    }

    static class DeleteCallback implements ProcedureCallback {

        Pair<Long, Long> m_pair;
        Queue<Pair<Long, Long>> m_queue;

        public DeleteCallback(Queue<Pair<Long, Long>> q, Pair<Long, Long> p) {
            m_pair = p;
            m_queue = q;
        }

        @Override
        public void clientCallback(ClientResponse response)
                throws Exception {
            if (response.getStatus() != ClientResponse.SUCCESS) {
                System.out.println(response.getStatusString());
                return;
            }
            m_queue.remove(m_pair);

        }
    }

}
