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
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;

public class CheckData {
    static Queue<Pair<String, String>> m_queue;
    static Queue<Pair<String, String>> m_delete_queue;
    static String m_select;
    static String m_delete;
    static String m_max;
    Client m_client;
    private static final int KEY = 0;
    private static final int VALUE = 1;

    private static final String PARTITIONED_SELECT = "PARTITIONED.select";
    private static final String PARTITIONED_DELETE = "PARTITIONED.delete";
    private static final String REPLICATED_SELECT = "REPLICATED.select";
    private static final String REPLICATED_DELETE = "REPLICATED.delete";
    private static final String REPLICATED_MAX = "SelectMaxTimeReplicated";
    private static final String PARTITIONED_MAX = "SelectMaxTimePartitioned";

    public CheckData(Queue<Pair<String, String>> q, Queue<Pair<String, String>> dq, Client c, boolean partitioned) {
        m_client = c;
        m_queue = q;
        m_delete_queue = dq;
        m_select = partitioned ? PARTITIONED_SELECT : REPLICATED_SELECT;
        m_delete = partitioned ? PARTITIONED_DELETE : REPLICATED_DELETE;
        m_max = partitioned ? PARTITIONED_MAX : REPLICATED_MAX;
    }

    public void processQueue() {
        while (m_queue.size() > 0 || m_delete_queue.size() > 0) {
            Pair<String, String> p = m_queue.poll();

            try {
                if (p != null) {
                    String key = p.getFirst();
                    boolean ret = m_client.callProcedure(new SelectCallback(m_queue, p, key), m_select, key);
                    if (!ret) {
                        System.out.println("Select call failed!");
                    }
                }
                Pair<String, String> p2 = m_delete_queue.poll();
                if (p2 != null) {
                    boolean ret = m_client.callProcedure(new DeleteCallback(m_delete_queue, p2), m_delete, p2.getFirst());
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

    public long maxInsertTime() {
    		ClientResponse response = null;
			try {
				response = m_client.callProcedure(m_max);
			} catch (NoConnectionsException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ProcCallException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    		VoltTable[] countQueryResult = response.getResults();
            VoltTable data = countQueryResult[0];
            if (data.asScalarLong() == VoltType.NULL_BIGINT)
                return 0;
            return data.asScalarLong();
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
                System.out.println(response.getStatusString());
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
                System.out.println("Pair from DB: " + key + ", " + value);
                System.out.println("Pair from queue: " + m_pair.getFirst() + ", " + m_pair.getSecond());
                AsyncBenchmark.rowsMismatch.incrementAndGet();
            }
        }

        private List<String> getDataFromResponse(ClientResponse response) {
            List<String> m_pair = new ArrayList<String>();
            //Long[] m_pairString = new Long[0];
            VoltTable[] m_results = response.getResults();
            if (m_results.length == 0) {
                System.out.println("zero length results");
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
                System.out.println(response.getStatusString());
                return;
            }
            m_queue.remove(m_pair);

        }
    }
}
