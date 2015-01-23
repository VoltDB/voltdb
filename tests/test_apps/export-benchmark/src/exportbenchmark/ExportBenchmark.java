package exportbenchmark;

import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.VoltBulkLoader.BulkLoaderFailureCallBack;
import org.voltdb.client.VoltBulkLoader.VoltBulkLoader;

public class ExportBenchmark {
    
    private Client client;
    
    static class VoltDBLog4JAppenderCallback implements BulkLoaderFailureCallBack {
        @Override
        public void failureCallback(Object rowHandle, Object[] fieldList, ClientResponse response) {
            System.err.println("Log insertion into VoltDB failed:");
            System.err.println(response.getStatusString());
        }
    }
    
    /**
     * Creates a new instance of the test to be run.
     * Establishes a client connection to a voltdb server, which should already be running
     */
    public ExportBenchmark() {
        ClientConfig clientConfig = new ClientConfig("", "");
        clientConfig.setReconnectOnConnectionLoss(true);
        clientConfig.setClientAffinity(true);
        client = ClientFactory.createClient(clientConfig);
    }

    /**
     * Runs the export benchmark test
     */
    private void runTest() {
        System.out.println("Test initialization");
        
        // Server connection
        VoltBulkLoader bulkLoader = null;
        try {
            client.createConnection("localhost");
            bulkLoader = client.getNewBulkLoader("valuesToExport", 50, new VoltDBLog4JAppenderCallback());
        }
        catch (Exception e) {
            System.err.printf("Connection to VoltDB failed\n" + e.getMessage());
            System.exit(1);
        }
        System.out.println("Initialization complete");
        
        
        // Insert objects
        long startTime = System.nanoTime();
        try {
            System.out.println("Inserting objects");
            Object rowHandle = null;
            for (int i = 0; i < 1000000; i++) {
                bulkLoader.insertRow(rowHandle, i, 42);
            }
            bulkLoader.drain();
            System.out.println("Object insertion complete");
        } catch (Exception e) {
            System.err.println("Couldn't insert into VoltDB\n" + e.getMessage());
            System.exit(1);
        }
        
        // Wait until export is done
        try {
            while (true) {
                VoltTable results = client.callProcedure("@Statistics", "TABLE", 0).getResults()[0];
                results.advanceRow();
                if (results.getLong("TUPLE_COUNT") == 1000000) {
                    break;
                }
                Thread.sleep(100);
            }
        } catch (Exception e) {
            System.err.println("Unable to analyze export table");
            System.err.println(e.getMessage());
            System.exit(1);
        }
        
        // See how much time elapsed
        long estimatedTime = System.nanoTime() - startTime;
        System.out.println("Export time elapsed (ms) for 1,000,000 objects: " + estimatedTime/1000000);
    }

    /**
     * Main routine creates a benchmark instance and kicks off the run method.
     *
     * @param args Command line arguments.
     * @throws Exception if anything goes wrong.
     */
    public static void main(String[] args) throws Exception {
        ExportBenchmark bench = new ExportBenchmark();
        bench.runTest();
    }
}