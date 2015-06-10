package socketimporter;

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
	static Queue<Pair<Long,Long>> m_queue;
	Client m_client;
	private static final int VALUE = 1;
    private static final int KEY = 0;

	private static final String MY_SELECT_PROCEDURE = "IMPORTTABLE.select";


	public CheckData(Queue<Pair<Long,Long>> q, Client c) {
		m_client = c;
		m_queue = q;
	}

	public void processQueue() {
		while (m_queue.size() > 0) {
			Pair<Long,Long> p = m_queue.poll();

			Long key = p.getFirst();
			try {
				boolean ret = m_client.callProcedure(new SelectCallback(m_queue, p, key), MY_SELECT_PROCEDURE, key);
				if (! ret) {
					System.out.println("Select call failed!");
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

        Pair<Long,Long> m_pair;
        Queue<Pair<Long,Long>> m_queue;
        Long m_key;

        public SelectCallback(Queue<Pair<Long,Long>> q, Pair<Long,Long> p, Long key) {
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
			} else {
				// push the tuple back onto the queue we can try again
				m_queue.offer(m_pair);
				return;
			}

			if (! value.equals(m_pair.getSecond())){
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

}
