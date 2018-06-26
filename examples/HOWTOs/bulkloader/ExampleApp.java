import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import org.voltdb.*;
import org.voltdb.client.*;
import org.voltdb.client.VoltBulkLoader.*;
import org.voltdb.types.*;

public class ExampleApp {

    public static Random rand = new Random();

    public static AtomicLong errors = new AtomicLong(0);
    public static AtomicLong commits = new AtomicLong(0);
    public static int testSize = 1000000;

    public static void handleResponse(ClientResponse cr) {
        if (cr.getStatus() == ClientResponse.SUCCESS) {
            commits.incrementAndGet();
        } else {
            errors.incrementAndGet();
            System.err.println(cr.getStatusString());
        }
    }

    // Implement a BulkLoaderFailureCallback for your BulkLoader
    public static class SessionBulkloaderFailureCallback implements BulkLoaderFailureCallBack {
        @Override
        public void failureCallback(Object rowHandle, Object[] fieldList, ClientResponse cr) {
            handleResponse(cr);
        }
    }

    // Implement a BulkLoaderSuccessCallback for your BulkLoader
    public static class SessionBulkloaderSuccessCallback implements BulkLoaderSuccessCallback {
        @Override
        public void success(Object rowHandle, ClientResponse cr) {
            handleResponse(cr);
        }
    }



    // Implement ProcedureCallback for asynchronous procedure calls
    public static class DefaultCallback implements ProcedureCallback {
        @Override
        public void clientCallback(ClientResponse cr) {
            handleResponse(cr);
        }
    }



    public static void testDefaultProcedure(Client client) throws Exception {

        System.out.println("Benchmarking APP_SESSION.insert procedure calls...");
        long startNanos = System.nanoTime();
        for (int i=0; i<testSize; i++) {

            int appid = rand.nextInt(50);
            int deviceid = rand.nextInt(1000000);
            TimestampType ts = new TimestampType();

            client.callProcedure(new DefaultCallback(),
                                 "APP_SESSION.insert",
                                 appid,
                                 deviceid,
                                 ts
                                 );
        }
        client.drain(); // wait for all responses to return
        double elapsedSeconds = (System.nanoTime() - startNanos)/1000000000.0;
        int tps = (int)(testSize/elapsedSeconds);
        System.out.println("Loaded "+testSize+" records in "+elapsedSeconds+" seconds ("+tps+" rows/sec)");
        System.out.println("  commits: " + commits.get());
        System.out.println("  errors: " + errors.get());
        commits.set(0);
        errors.set(0);
    }


    public static void testBulkLoader(Client client) throws Exception {

        System.out.println("Benchmarking with VoltBulkLoader...");

        // Get a BulkLoader for the table we want to load, with a given batch size and one callback handles failures for any failed batches
        int batchSize = 1000;
        boolean upsertMode = false;
        VoltBulkLoader bulkLoader = client.getNewBulkLoader("app_session",
                                                            batchSize,
                                                            upsertMode,
                                                            new SessionBulkloaderFailureCallback(),
                                                            new SessionBulkloaderSuccessCallback());

        long startNanos = System.nanoTime();
        for (int i=0; i<testSize; i++) {

            Integer rowId = new Integer(i);

            int appid = rand.nextInt(50);
            int deviceid = rand.nextInt(1000000);
            TimestampType ts = new TimestampType();
            Object[] row = {appid, deviceid, ts};

            bulkLoader.insertRow(rowId, row);
        }
        bulkLoader.drain();
        client.drain();
        double elapsedSeconds = (System.nanoTime() - startNanos)/1000000000.0;
        int tps = (int)(testSize/elapsedSeconds);
        System.out.println("Loaded "+testSize+" records in "+elapsedSeconds+" seconds ("+tps+" rows/sec)");
        System.out.println("  commits: " + commits.get());
        System.out.println("  errors: " + errors.get());
        commits.set(0);
        errors.set(0);

        bulkLoader.close();
    }


    public static void main(String[] args) throws Exception {

        /*
         * Instantiate a client and connect to the database.
         */
        org.voltdb.client.Client client;
        client = ClientFactory.createClient();
        client.createConnection("localhost");


        testDefaultProcedure(client);

        testBulkLoader(client);

        client.close();

    }
}
