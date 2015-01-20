package exportbenchmark;

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
    
    public ExportBenchmark() {
        ClientConfig clientConfig = new ClientConfig("", "");
        clientConfig.setReconnectOnConnectionLoss(true);
        clientConfig.setClientAffinity(true);
        client = ClientFactory.createClient(clientConfig);
    }

    private void runTest() {
        System.out.println("Test initialization");
        
        // Server connection
        VoltBulkLoader bulkLoader = null;
        try {
            client.createConnection("localhost");
            bulkLoader = client.getNewBulkLoader("log4j", 1, new VoltDBLog4JAppenderCallback());
        }
        catch (Exception e) {
            System.err.printf("Connection to VoltDB failed\n" + e.getMessage());
            System.exit(1);
        }
        
        
        // Insert objects
        try {
            System.out.println("Inserting objects");
            Object rowHandle = null;
            for (int i = 0; i < 1000000; i++) {
                bulkLoader.insertRow(rowHandle, i);
            }
            System.out.println("Object insertion complete");
        } catch (Exception e) {
            System.err.println("Couldn't insert into VoltDB\n" + e.getMessage());
            System.exit(1);
        }
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