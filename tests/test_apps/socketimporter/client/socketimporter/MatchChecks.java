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
//package socketimporter.client.socketimporter;
//

//
//public class InsertExport {
//	final Client m_client;
//
//	public InsertExport(Client client) {
//		m_client = client;
//	}
//
//	public void insertExport(long key, long value) {
//		try {
//			m_client.callProcedure(new InsertCallback(), "InsertExport", key, value);
//		} catch (IOException e) {
//			System.out.println("Exception calling stored procedure InsertExport");
//			e.printStackTrace();
//		}
//	}
//
//	static class InsertCallback implements ProcedureCallback {
//
//		@Override
//		public void clientCallback(ClientResponse clientResponse)
//				throws Exception {
//			if (clientResponse.getStatus() != ClientResponse.SUCCESS) {
//				System.err.println(clientResponse.getStatusString());
//			}
//		}
//
//	}
//}

package socketimporter.client.socketimporter;

import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;

import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;

public class MatchChecks {
    static Client client;

    static void dbconnect(String servers) throws InterruptedException, Exception {
        System.out.println("Connecting to VoltDB Interface...");

        String[] serverArray = servers.split(",");
        client = ClientFactory.createClient();
        for (String server : serverArray) {
            System.out.println("..." + server);
            client.createConnection(server);
        }
    }

    static class DeleteCallback implements ProcedureCallback {
        @Override
        public void clientCallback(ClientResponse response) {

                // Make sure the procedure succeeded. If not,
                // report the error.
            if (response.getStatus() != ClientResponse.SUCCESS) {
                System.err.println(response.getStatusString());
              }
         }
    }

	public static void main(String[] args) {
		long interval = 5000;
		String servers = "localhost";
		long mirroRowCount = 0;

		try {
			dbconnect(servers);
		} catch (Exception e) {
			e.printStackTrace();
		}



		final Timer timer = new Timer();
		timer.scheduleAtFixedRate(new TimerTask() {
		    private int count = 20;
		    private long mirrorRowCount = 0;

			@Override
		    public void run() {
				mirrorRowCount = getMirrorTableRowCount();
				System.out.println("Loop countdown: " + count);
				System.out.println("\tDelete rows: " + findAndDeleteMatchingRows());
				System.out.println("\tMirror table row count: " + mirrorRowCount);
		    	if (count-- == 0 || mirrorRowCount == 0) {
		    		timer.cancel();
		    		timer.purge();
		    	}
		    }
		}, 0, interval);
	}

	private static long getMirrorTableRowCount() {
		// check row count in mirror table -- the "master" of what should come back
		// eventually via import
		long mirrorRowCount = 0;
		try {
			VoltTable[] countQueryResult = client.callProcedure("CountMirror").getResults();
			mirrorRowCount = countQueryResult[0].asScalarLong();
		} catch (IOException | ProcCallException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("Mirror table row count: " + mirrorRowCount);
		return mirrorRowCount;
	}

	private static long findAndDeleteMatchingRows() {
		long rows = 0;
		VoltTable results = null;

		try {
			results = client.callProcedure("MatchRows").getResults()[0];
		} catch (Exception e) {
		     e.printStackTrace();
		     System.exit(-1);
		}

		System.out.println("getRowCount(): " + results.getRowCount());
		while (results.advanceRow()) {
			long key = results.getLong(0);
			// System.out.println("Key: " + key);
			try {
				client.callProcedure(new DeleteCallback(), "DeleteRows", key);
			} catch (IOException e) {
				e.printStackTrace();
			}
			rows++;
		}
		return rows;
	}

}

